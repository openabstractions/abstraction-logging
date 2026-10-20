package logging

import (
	"context"
	"errors"
	"fmt"
	timestampwire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
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

// ErrWriteTimeout marks a delivery that did not return within
// AsyncOptions.WriteTimeout, or that waited that long for an earlier timed-out
// delivery to return.
var ErrWriteTimeout = errors.New("logging: asynchronous sink delivery exceeded its write timeout")

const (
	// DefaultAsyncCapacity is the queue bound when AsyncOptions.Capacity is zero.
	DefaultAsyncCapacity = 1024
	// DefaultWriteTimeout bounds each delivery when AsyncOptions.WriteTimeout is zero.
	DefaultWriteTimeout = time.Duration(timestampwire.DefaultWriteTimeoutMs) * time.Millisecond
	// DefaultInitialBackoff is the wait before the first retry [LOG-S12].
	DefaultInitialBackoff = time.Duration(timestampwire.DefaultInitialBackoffMs) * time.Millisecond
	// DefaultMaxBackoff caps the doubling wait between retries [LOG-S12].
	DefaultMaxBackoff = time.Duration(timestampwire.DefaultMaxBackoffMs) * time.Millisecond
	// BackoffJitter varies each wait by up to this fraction either way.
	BackoffJitter = 0.2
	// GapMessage is the message of the WARN record a recovery delivers [LOG-S13].
	GapMessage = timestampwire.WriterGapMessage
	// Preserve the public untyped constants and require canonical-value equality.
	_ = uint(DefaultAsyncCapacity - timestampwire.DefaultAsyncCapacity)
	_ = uint(timestampwire.DefaultAsyncCapacity - DefaultAsyncCapacity)
	_ = uint(BackoffJitter*100 - timestampwire.WriterBackoffJitterPercent)
	_ = uint(timestampwire.WriterBackoffJitterPercent - BackoffJitter*100)
)

// Rebinder is a sink that can resolve its service again. AsyncSink calls
// Rebind after a failed delivery and before each retry.
type Rebinder interface {
	Rebind(context.Context) error
}

// AsyncOptions configures NewAsyncSink. Callbacks run on the goroutine that
// observed the transition, outside the sink's lock, before the sink continues;
// they must not block. With none of the three callbacks set, the sink reports
// each transition as one line on standard error [LOG-S13].
type AsyncOptions struct {
	// Capacity is the number of records the queue holds, counting the record
	// being delivered; zero means DefaultAsyncCapacity.
	Capacity int
	// WriteTimeout bounds each delivery and each rebind; zero means
	// DefaultWriteTimeout, and a negative value leaves deliveries unbounded.
	//
	// A bounded delivery runs on a goroutine of its own. A delivery that has not
	// returned in time is cancelled (for an inner sink with WriteContext) and is
	// a failed delivery with ErrWriteTimeout. An inner sink that ignores
	// cancellation keeps running: the next delivery waits for it within its own
	// timeout, so the inner sink is never called concurrently. A record whose
	// delivery timed out may still arrive, and its retry delivers it again.
	WriteTimeout time.Duration
	// InitialBackoff and MaxBackoff shape the wait between retries; zero means
	// DefaultInitialBackoff and DefaultMaxBackoff.
	InitialBackoff, MaxBackoff time.Duration
	// Program names the hop-0 claim of the gap record.
	Program string
	// Rebind resolves the service again. Nil uses the inner sink's Rebind when
	// it is a Rebinder, and otherwise retries the same inner sink.
	Rebind func(context.Context) error
	// OnOverflow runs on the first dropped record since the queue was last empty:
	// once per run of drops, however often the queue refills before it drains.
	OnOverflow func(AsyncCounts)
	// OnFailure runs when a delivery fails after the previous one succeeded,
	// once the first rebind attempt returned. The error is that rebind's error
	// when it failed, and the delivery's error otherwise.
	OnFailure func(error, AsyncCounts)
	// OnRecovery runs when a retry delivered the gap record.
	OnRecovery func(AsyncCounts)
}

// AsyncCounts is a snapshot of an AsyncSink [LOG-S13]. Every record handed to
// Write is exactly one of Dropped or Accepted; every accepted record ends as
// exactly one of Written, Native, Failed or Abandoned, or is still Queued.
// Native counts the records a native handler took while the machine offered
// no logging service [LOG-S15].
type AsyncCounts struct {
	Accepted, Dropped          uint64
	Written, Failed, Abandoned uint64
	Native                     uint64
	Queued                     int
	Failing                    bool
	LastError                  string
}

// AsyncSink delivers records to another Sink on one background goroutine, so a
// slow, stalled or restarting service costs the logging caller nothing
// [LOG-S12]. The queue is bounded: a full queue drops the newest record, counts
// it and returns ErrQueueFull. A failed delivery keeps its record at the head of
// the queue, rebinds, and retries with backoff until it is delivered or Close
// reaches its deadline. Records reach the inner sink in the order they were
// accepted, and go to no other sink.
type AsyncSink struct {
	inner  Sink
	opts   AsyncOptions
	rebind func(context.Context) error
	wake   chan struct{}
	done   chan struct{}
	stop   context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	ring        []Record
	head, size  int
	closed      bool
	overflowing bool
	since       time.Time // the current failure began, or the previous gap record's until while draining
	until       time.Time // the until of the latest gap record built
	lost        uint64    // dropped records no delivered gap record reported
	reported    uint64    // the dropped count the latest gap record carries
	draining    bool      // a recovery happened and the queue has not been empty since
	counts      AsyncCounts
}

// NewAsyncSink starts the delivery goroutine. Call Close to stop it.
func NewAsyncSink(inner Sink, opts AsyncOptions) *AsyncSink {
	if opts.Capacity <= 0 {
		opts.Capacity = int(timestampwire.DefaultAsyncCapacity)
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = DefaultWriteTimeout
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = DefaultInitialBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = DefaultMaxBackoff
	}
	if opts.OnOverflow == nil && opts.OnFailure == nil && opts.OnRecovery == nil {
		opts.OnOverflow, opts.OnFailure, opts.OnRecovery = reportOverflow, reportFailure, reportRecovery
	}
	rebind := opts.Rebind
	if rebinder, ok := inner.(Rebinder); ok && rebind == nil {
		rebind = rebinder.Rebind
	}
	stop, cancel := context.WithCancel(context.Background())
	s := &AsyncSink{inner: inner, opts: opts, rebind: rebind, wake: make(chan struct{}, 1), done: make(chan struct{}),
		stop: stop, cancel: cancel, ring: make([]Record, opts.Capacity)}
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
	if s.size < len(s.ring) {
		s.ring[(s.head+s.size)%len(s.ring)] = r
		s.size++
		s.counts.Accepted++
		s.mu.Unlock()
		s.signal()
		return nil
	}
	s.counts.Dropped++
	s.lost++
	first := !s.overflowing
	s.overflowing = true
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
	c.Queued = s.size
	return c
}

func (s *AsyncSink) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Close stops accepting records, delivers and retries the queue until ctx is
// done, and returns the counts at that point. At the deadline it cancels the
// delivery or wait in progress, counts the record at the head as Failed and the
// records behind it as Abandoned, and returns ErrCloseDeadline with their number.
//
// With a bounded WriteTimeout, Close then waits for the delivery goroutine to
// account for every record, which never waits on the inner sink, and the counts
// are final. Without one, a delivery that ignores cancellation may still be in
// flight and in none of Written, Failed or Abandoned. Close is safe to call more
// than once.
func (s *AsyncSink) Close(ctx context.Context) (AsyncCounts, error) {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.signal()
	select {
	case <-s.done:
		return s.Counts(), nil
	case <-ctx.Done():
	}
	s.mu.Lock()
	remaining := s.size
	s.mu.Unlock()
	s.cancel()
	if s.opts.WriteTimeout > 0 {
		<-s.done
	}
	return s.Counts(), fmt.Errorf("%w: %d queued records undelivered: %v", ErrCloseDeadline, remaining, ctx.Err())
}

func (s *AsyncSink) run() {
	defer close(s.done)
	deliverAny := func(r Record) error { return s.deliver(s.stop, r) }
	if s.opts.WriteTimeout > 0 {
		bounded := &boundedDelivery{sink: s, attempts: make(chan attempt)}
		go bounded.loop()
		defer close(bounded.attempts)
		deliverAny = bounded.deliver
	}
	// A record the native handler took is delivered, and counted Native.
	native := false
	deliver := func(r Record) error {
		err := deliverAny(r)
		native = errors.Is(err, errDeliveredNative)
		if native {
			return nil
		}
		return err
	}
	for {
		if err := s.reportDrained(deliver); err != nil {
			if s.stop.Err() != nil || !s.retry(deliver, err) {
				s.giveUp()
				return
			}
			continue
		}
		r, ok := s.next()
		if !ok {
			return
		}
		err := deliver(r)
		if err == nil {
			s.mu.Lock()
			s.pop()
			if native {
				s.counts.Native++
			} else {
				s.counts.Written++
			}
			if !s.draining {
				s.lost = 0
			}
			s.mu.Unlock()
			continue
		}
		if s.stop.Err() != nil || !s.retry(deliver, err) {
			s.giveUp()
			return
		}
	}
}

// reportDrained ends a drain [LOG-S13]. The first time the queue is empty after
// a recovery, the records dropped after the recovery's gap record was built are
// reported by a later gap record, delivered here. It returns that delivery's
// error.
func (s *AsyncSink) reportDrained(deliver func(Record) error) error {
	s.mu.Lock()
	if !s.draining || s.size > 0 {
		s.mu.Unlock()
		return nil
	}
	if s.lost == 0 {
		s.draining = false
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	if err := deliver(s.gapRecord()); err != nil {
		return err
	}
	s.mu.Lock()
	s.lost -= s.reported
	s.since = s.until
	s.mu.Unlock()
	return nil
}

// next waits for a record at the head of the queue. It reports false once the
// sink is closed and empty, or stopped, abandoning what is still queued.
func (s *AsyncSink) next() (Record, bool) {
	for {
		s.mu.Lock()
		switch {
		case s.stop.Err() != nil:
			s.counts.Abandoned += uint64(s.size)
			s.clear()
			s.mu.Unlock()
			return Record{}, false
		case s.size > 0:
			r := s.ring[s.head]
			s.mu.Unlock()
			return r, true
		case s.closed:
			s.mu.Unlock()
			return Record{}, false
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-s.stop.Done():
		}
	}
}

// retry runs one failure: it rebinds, reports the failure, and retries with
// backoff until the gap record is delivered. It reports false when the sink was
// stopped first.
func (s *AsyncSink) retry(deliver func(Record) error, cause error) bool {
	s.mu.Lock()
	s.counts.Failing = true
	s.counts.LastError = cause.Error()
	if !s.draining {
		s.since = time.Now()
	}
	s.mu.Unlock()
	if err := s.rebindOnce(); err != nil {
		cause = err
	}
	s.mu.Lock()
	s.counts.LastError = cause.Error()
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	if s.opts.OnFailure != nil {
		s.opts.OnFailure(cause, snapshot)
	}
	delay := s.opts.InitialBackoff
	for {
		timer := time.NewTimer(jittered(delay))
		select {
		case <-timer.C:
		case <-s.stop.Done():
			timer.Stop()
			return false
		}
		delay = min(2*delay, s.opts.MaxBackoff)
		err := s.rebindOnce()
		if err == nil {
			err = deliver(s.gapRecord())
		}
		if s.stop.Err() != nil {
			return false
		}
		if err != nil {
			s.mu.Lock()
			s.counts.LastError = err.Error()
			s.mu.Unlock()
			continue
		}
		s.mu.Lock()
		s.counts.Failing = false
		s.lost -= s.reported
		s.since = s.until
		s.draining = true
		snapshot := s.snapshotLocked()
		s.mu.Unlock()
		if s.opts.OnRecovery != nil {
			s.opts.OnRecovery(snapshot)
		}
		return true
	}
}

func (s *AsyncSink) rebindOnce() error {
	if s.rebind == nil {
		return nil
	}
	ctx, cancel := s.stop, context.CancelFunc(func() {})
	if s.opts.WriteTimeout > 0 {
		ctx, cancel = context.WithTimeout(s.stop, s.opts.WriteTimeout)
	}
	defer cancel()
	return s.rebind(ctx)
}

// gapRecord is the WARN record whose delivery is the recovery, or that reports
// the records dropped while the queue drained after one [LOG-S13].
func (s *AsyncSink) gapRecord() Record {
	s.mu.Lock()
	since, lost := s.since, s.lost
	until := time.Now()
	s.reported, s.until = lost, until
	s.mu.Unlock()
	return Record{
		Schema: SchemaVersion, Time: At(until), Level: LevelWarn, Msg: GapMessage,
		Identity: Identity{Claim(s.opts.Program)},
		Attrs: map[string]string{
			"dropped": strconv.FormatUint(lost, 10),
			"since":   timestampwire.FormatTimestamp(since),
			"until":   timestampwire.FormatTimestamp(until),
		},
	}
}

// giveUp counts the head record Failed and the records behind it Abandoned.
func (s *AsyncSink) giveUp() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.size > 0 {
		s.pop()
		s.counts.Failed++
	}
	s.counts.Abandoned += uint64(s.size)
	s.clear()
}

func (s *AsyncSink) pop() {
	s.ring[s.head] = Record{}
	s.head = (s.head + 1) % len(s.ring)
	s.size--
	if s.size == 0 {
		s.overflowing = false
	}
}

func (s *AsyncSink) clear() {
	for s.size > 0 {
		s.pop()
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

// jittered varies d by up to BackoffJitter either way.
func jittered(d time.Duration) time.Duration {
	const jitter = float64(timestampwire.WriterBackoffJitterPercent) / 100
	return time.Duration(float64(d) * (1 - jitter + 2*jitter*rand.Float64()))
}

// failureClass names a failure on the default reporter's line: the resolution
// status, timeout, or write_failed.
func failureClass(err error) string {
	var refused *ResolutionError
	switch {
	case errors.As(err, &refused):
		return string(refused.Status)
	case errors.Is(err, ErrWriteTimeout), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "write_failed"
	}
}

// reportTo is the default reporter's stream, read at each report.
var reportTo = func() io.Writer { return os.Stderr }

func reportFailure(err error, c AsyncCounts) {
	//unchecked: default reporter with no return value to report a write failure through
	fmt.Fprintf(reportTo(), "abstraction.logging: service unreachable (%s); %d queued, %d dropped\n", failureClass(err), c.Queued, c.Dropped)
}

func reportOverflow(c AsyncCounts) {
	//unchecked: default reporter with no return value to report a write failure through
	fmt.Fprintf(reportTo(), "abstraction.logging: queue full, dropping newest records; %d queued, %d dropped\n", c.Queued, c.Dropped)
}

func reportRecovery(c AsyncCounts) {
	//unchecked: default reporter with no return value to report a write failure through
	fmt.Fprintf(reportTo(), "abstraction.logging: delivering again; %d dropped\n", c.Dropped)
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
	// delivery's turn can come first. It waits for the earlier one within its
	// own timeout, so a sink that honours cancellation loses nothing to that race.
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
