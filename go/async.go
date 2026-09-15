package logging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrQueueFull is returned by AsyncSink.Write when the queue is full. The
// record is dropped and counted; nothing blocks the caller.
var ErrQueueFull = errors.New("logging: asynchronous sink queue full; record dropped")

// ErrSinkClosed is returned by AsyncSink.Write after Close began.
var ErrSinkClosed = errors.New("logging: asynchronous sink closed")

// ErrCloseDeadline is returned by AsyncSink.Close when queued records were still
// undelivered at the caller's deadline.
var ErrCloseDeadline = errors.New("logging: asynchronous sink close deadline reached")

// ErrWriteTimeout marks a record counted as Failed because its delivery did not
// return within AsyncOptions.WriteTimeout, or because an earlier timed-out
// delivery had still not returned within the record's own write timeout.
var ErrWriteTimeout = errors.New("logging: asynchronous sink delivery exceeded its write timeout")

// DefaultAsyncCapacity is the queue bound when AsyncOptions.Capacity is zero.
const DefaultAsyncCapacity = 1024

// AsyncOptions configures NewAsyncSink. Callbacks run on the goroutine that
// observed the transition, outside the sink's lock; they must not block.
type AsyncOptions struct {
	// Capacity is the number of records the queue holds; zero means
	// DefaultAsyncCapacity.
	Capacity int
	// WriteTimeout bounds each delivery. Zero leaves deliveries unbounded, and a
	// hung inner sink then holds the delivery goroutine until Close.
	//
	// When set, deliveries run on a goroutine of their own. A delivery that has
	// not returned in time is cancelled (for an inner sink with WriteContext),
	// counted as Failed with ErrWriteTimeout and reported through OnFailure, and
	// the next record is taken. An inner sink that ignores cancellation keeps
	// running: a record whose turn comes before it returns waits for it within
	// its own WriteTimeout and then fails with ErrWriteTimeout, so the inner sink
	// is never called concurrently. A record whose delivery timed out may still
	// arrive.
	WriteTimeout time.Duration
	// OnOverflow runs when a record is dropped after one was accepted: once per
	// run of drops.
	OnOverflow func(AsyncCounts)
	// OnFailure runs when a delivery fails after the previous one succeeded (or
	// before any did): once per run of failures.
	OnFailure func(error, AsyncCounts)
	// OnRecovery runs on the first successful delivery after a failure.
	OnRecovery func(AsyncCounts)
}

// AsyncCounts is a snapshot of an AsyncSink. Every record handed to Write is
// exactly one of Dropped, or Accepted; every accepted record ends as exactly one
// of Written, Failed or Abandoned, or is still Queued or in flight.
type AsyncCounts struct {
	Accepted, Dropped          uint64
	Written, Failed, Abandoned uint64
	Queued                     int
	Overflowing, Failing       bool
	LastError                  string
}

// AsyncSink delivers records to another Sink on one background goroutine, so a
// slow or stalled service costs the logging caller nothing. The queue is
// bounded: a full queue drops the newest record, counts it and returns
// ErrQueueFull. Deliveries keep the same failure accounting as a synchronous
// sink. Records reach the inner sink in the order they were accepted.
type AsyncSink struct {
	inner  Sink
	opts   AsyncOptions
	queue  chan Record
	done   chan struct{}
	stop   context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	counts AsyncCounts
}

// NewAsyncSink starts the delivery goroutine. Call Close to stop it.
func NewAsyncSink(inner Sink, opts AsyncOptions) *AsyncSink {
	if opts.Capacity <= 0 {
		opts.Capacity = DefaultAsyncCapacity
	}
	stop, cancel := context.WithCancel(context.Background())
	s := &AsyncSink{inner: inner, opts: opts, queue: make(chan Record, opts.Capacity), done: make(chan struct{}), stop: stop, cancel: cancel}
	go s.run()
	return s
}

// Write enqueues without blocking. It returns ErrQueueFull when the record was
// dropped and ErrSinkClosed after Close began.
func (s *AsyncSink) Write(r Record) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSinkClosed
	}
	select {
	case s.queue <- r:
		s.counts.Accepted++
		s.counts.Overflowing = false
		s.mu.Unlock()
		return nil
	default:
	}
	s.counts.Dropped++
	first := !s.counts.Overflowing
	s.counts.Overflowing = true
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	if first && s.opts.OnOverflow != nil {
		s.opts.OnOverflow(snapshot)
	}
	return ErrQueueFull
}

// WriteContext is Write: enqueueing never waits, so ctx has nothing to bound.
func (s *AsyncSink) WriteContext(_ context.Context, r Record) error { return s.Write(r) }

// Counts returns a snapshot.
func (s *AsyncSink) Counts() AsyncCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *AsyncSink) snapshotLocked() AsyncCounts {
	c := s.counts
	c.Queued = len(s.queue)
	return c
}

// Close stops accepting records, waits for the queue to drain until ctx is
// done, and returns the counts at that point. At the deadline it cancels the
// delivery in flight, abandons the records still queued, and returns
// ErrCloseDeadline with their number.
//
// With a WriteTimeout, Close then waits for the delivery goroutine to account
// for every record, which never waits on the inner sink, and the counts are
// final. Without one, a delivery that ignores cancellation may still be in
// flight and in none of Written, Failed or Abandoned. Close is safe to call more
// than once.
func (s *AsyncSink) Close(ctx context.Context) (AsyncCounts, error) {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return s.Counts(), nil
	case <-ctx.Done():
	}
	s.mu.Lock()
	remaining := len(s.queue)
	s.mu.Unlock()
	s.cancel()
	if s.opts.WriteTimeout > 0 {
		<-s.done
	}
	return s.Counts(), fmt.Errorf("%w: %d queued records abandoned: %v", ErrCloseDeadline, remaining, ctx.Err())
}

func (s *AsyncSink) run() {
	defer close(s.done)
	var bounded *boundedDelivery
	if s.opts.WriteTimeout > 0 {
		bounded = &boundedDelivery{sink: s, attempts: make(chan attempt)}
		go bounded.loop()
		defer close(bounded.attempts)
	}
	for r := range s.queue {
		if s.stop.Err() != nil {
			s.mu.Lock()
			s.counts.Abandoned++
			s.mu.Unlock()
			continue
		}
		var err error
		if bounded != nil {
			err = bounded.deliver(r)
		} else {
			err = s.deliver(s.stop, r)
		}
		s.mu.Lock()
		var failed, recovered bool
		if err != nil {
			s.counts.Failed++
			failed = !s.counts.Failing
			s.counts.Failing = true
			s.counts.LastError = err.Error()
		} else {
			s.counts.Written++
			recovered = s.counts.Failing
			s.counts.Failing = false
		}
		snapshot := s.snapshotLocked()
		s.mu.Unlock()
		if failed && s.opts.OnFailure != nil {
			s.opts.OnFailure(err, snapshot)
		}
		if recovered && s.opts.OnRecovery != nil {
			s.opts.OnRecovery(snapshot)
		}
	}
}

func (s *AsyncSink) deliver(ctx context.Context, r Record) error {
	if sink, ok := s.inner.(interface {
		WriteContext(context.Context, Record) error
	}); ok {
		return sink.WriteContext(ctx, r)
	}
	return s.inner.Write(r)
}

type attempt struct {
	ctx    context.Context
	record Record
	result chan error
}

// boundedDelivery calls the inner sink on a goroutine of its own, so the
// delivery goroutine can stop waiting for a delivery that exceeds the write
// timeout. It hands the inner sink one record at a time.
type boundedDelivery struct {
	sink     *AsyncSink
	attempts chan attempt
	pending  chan error // the result of a timed-out delivery that has not returned
}

func (b *boundedDelivery) loop() {
	for a := range b.attempts {
		a.result <- b.sink.deliver(a.ctx, a.record)
	}
}

func (b *boundedDelivery) deliver(r Record) error {
	timeout := b.sink.opts.WriteTimeout
	ctx, cancel := context.WithTimeout(b.sink.stop, timeout)
	defer cancel()
	// A cancelled delivery returns a moment after its timeout, and the next
	// record's turn can come first. The record waits for it within its own
	// timeout, so a sink that honours cancellation loses nothing to that race.
	if b.pending != nil {
		select {
		case <-b.pending:
			b.pending = nil
		case <-ctx.Done():
			if err := b.sink.stop.Err(); err != nil {
				return err
			}
			return fmt.Errorf("%w: an earlier delivery has not returned", ErrWriteTimeout)
		}
	}
	a := attempt{ctx: ctx, record: r, result: make(chan error, 1)}
	b.attempts <- a
	select {
	case err := <-a.result:
		return err
	case <-ctx.Done():
		b.pending = a.result
		if err := b.sink.stop.Err(); err != nil {
			return err
		}
		return fmt.Errorf("%w (%v)", ErrWriteTimeout, timeout)
	}
}
