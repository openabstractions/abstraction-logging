package logging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingSink struct {
	mu      sync.Mutex
	records []Record
	fail    atomic.Int32 // fail this many deliveries first
}

func (s *recordingSink) Write(r Record) error {
	if s.fail.Load() > 0 {
		s.fail.Add(-1)
		return errors.New("service refused")
	}
	s.mu.Lock()
	s.records = append(s.records, r)
	s.mu.Unlock()
	return nil
}

func (s *recordingSink) messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.records {
		out = append(out, r.Msg)
	}
	return out
}

// stalledSink blocks every delivery until release is closed, and reports the
// deliveries it started while its buffer has room.
type stalledSink struct {
	started chan string
	calls   atomic.Int32
	release chan struct{}
	honour  bool // stop waiting when the delivery context is cancelled
}

func newStalledSink(honour bool) *stalledSink {
	return &stalledSink{started: make(chan string, 64), release: make(chan struct{}), honour: honour}
}

func (s *stalledSink) Write(r Record) error { return s.WriteContext(context.Background(), r) }

func (s *stalledSink) WriteContext(ctx context.Context, r Record) error {
	s.calls.Add(1)
	select {
	case s.started <- r.Msg:
	default:
	}
	if !s.honour {
		ctx = context.Background()
	}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// plainStalled hides WriteContext so the worker cannot cancel it.
type plainStalled struct{ *stalledSink }

func (s plainStalled) Write(r Record) error {
	return s.stalledSink.WriteContext(context.Background(), r)
}

// quiet is a failure callback that keeps the default reporter off.
func quiet(error, AsyncCounts) {}

func asyncRecord(msg string) Record {
	return Record{Schema: SchemaVersion, Time: At(time.Unix(0, 0).UTC()), Msg: msg}
}

func closeWithin(t *testing.T, s *AsyncSink, d time.Duration) (AsyncCounts, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return s.Close(ctx)
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAsyncSinkDeliversInOrderAndDrainsOnClose(t *testing.T) {
	inner := &recordingSink{}
	s := NewAsyncSink(inner, AsyncOptions{Capacity: 256})
	var want []string
	for i := 0; i < 200; i++ {
		msg := fmt.Sprintf("r%03d", i)
		want = append(want, msg)
		if err := s.Write(asyncRecord(msg)); err != nil {
			t.Fatal(err)
		}
	}
	c, err := closeWithin(t, s, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := inner.messages(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order: %v", got)
	}
	if c.Accepted != 200 || c.Written != 200 || c.Dropped != 0 || c.Failed != 0 || c.Abandoned != 0 || c.Queued != 0 {
		t.Fatalf("counts returned by Close %+v", c)
	}
	if err := s.Write(asyncRecord("late")); !errors.Is(err, ErrSinkClosed) {
		t.Fatalf("write after close: %v", err)
	}
	if again, err := closeWithin(t, s, time.Second); err != nil || again != c {
		t.Fatalf("second close: %+v, %v", again, err)
	}
}

// The record being delivered stays in the queue and counts against its
// capacity [LOG-S12].
func TestAsyncSinkOverflowDropsNewestCountedAndReported(t *testing.T) {
	inner := newStalledSink(true)
	var overflows []AsyncCounts
	var mu sync.Mutex
	s := NewAsyncSink(inner, AsyncOptions{Capacity: 3, OnFailure: quiet, OnOverflow: func(c AsyncCounts) {
		mu.Lock()
		overflows = append(overflows, c)
		mu.Unlock()
	}})
	if err := s.Write(asyncRecord("in-flight")); err != nil {
		t.Fatal(err)
	}
	if got := <-inner.started; got != "in-flight" {
		t.Fatalf("started %q", got)
	}
	for _, m := range []string{"q1", "q2"} {
		if err := s.Write(asyncRecord(m)); err != nil {
			t.Fatalf("%s: %v", m, err)
		}
	}
	for _, m := range []string{"d1", "d2", "d3"} {
		if err := s.Write(asyncRecord(m)); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("%s: %v, want ErrQueueFull", m, err)
		}
	}
	c := s.Counts()
	if c.Accepted != 3 || c.Dropped != 3 || c.Queued != 3 {
		t.Fatalf("counts %+v", c)
	}
	mu.Lock()
	if len(overflows) != 1 || overflows[0].Dropped != 1 {
		t.Fatalf("overflow reports %+v", overflows)
	}
	mu.Unlock()

	close(inner.release)
	eventually(t, "queue to drain", func() bool { return s.Counts().Written == 3 })
	if err := s.Write(asyncRecord("after drain")); err != nil {
		t.Fatalf("accept after drain: %v", err)
	}
	final, err := closeWithin(t, s, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if final.Accepted != 4 || final.Written != 4 || final.Dropped != 3 {
		t.Fatalf("final counts %+v", final)
	}
}

func TestAsyncSinkStalledServiceCloseDeadlineCancelsAndAbandons(t *testing.T) {
	inner := newStalledSink(true)
	s := NewAsyncSink(inner, AsyncOptions{Capacity: 8, OnFailure: quiet})
	for i := 0; i < 5; i++ {
		if err := s.Write(asyncRecord(fmt.Sprint("s", i))); err != nil {
			t.Fatal(err)
		}
	}
	<-inner.started
	began := time.Now()
	_, err := closeWithin(t, s, 100*time.Millisecond)
	if !errors.Is(err, ErrCloseDeadline) {
		t.Fatalf("close: %v", err)
	}
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Fatalf("close waited %v past its deadline", elapsed)
	}
	c := s.Counts()
	if c.Failed != 1 || c.Abandoned != 4 || c.Written != 0 || c.Queued != 0 {
		t.Fatalf("counts %+v", c)
	}
	if calls := inner.calls.Load(); calls != 1 {
		t.Fatalf("%d deliveries started, want only the one cancelled at the deadline", calls)
	}
}

func TestAsyncSinkCloseReturnsAtDeadlineWhenInnerCannotBeCancelled(t *testing.T) {
	inner := newStalledSink(false)
	s := NewAsyncSink(plainStalled{inner}, AsyncOptions{Capacity: 4, WriteTimeout: -1, OnFailure: quiet})
	for i := 0; i < 3; i++ {
		if err := s.Write(asyncRecord(fmt.Sprint("u", i))); err != nil {
			t.Fatal(err)
		}
	}
	<-inner.started
	began := time.Now()
	if _, err := closeWithin(t, s, 50*time.Millisecond); !errors.Is(err, ErrCloseDeadline) {
		t.Fatalf("close: %v", err)
	}
	if time.Since(began) > 2*time.Second {
		t.Fatal("close blocked on an uncancellable delivery")
	}
	close(inner.release)
	eventually(t, "worker to finish", func() bool { c := s.Counts(); return c.Written+c.Abandoned == 3 })
	if c := s.Counts(); c.Written != 1 || c.Abandoned != 2 {
		t.Fatalf("counts %+v", c)
	}
}

// A failed delivery is retried, never counted Failed, and a recovery delivers
// the gap record ahead of the retried records [LOG-S12, LOG-S13].
func TestAsyncSinkFailureAndRecoveryReportedOncePerTransition(t *testing.T) {
	inner := &recordingSink{}
	inner.fail.Store(3)
	var failures, recoveries atomic.Int32
	var lastFailure atomic.Value
	s := NewAsyncSink(inner, AsyncOptions{
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond,
		OnFailure:  func(err error, c AsyncCounts) { failures.Add(1); lastFailure.Store(c) },
		OnRecovery: func(AsyncCounts) { recoveries.Add(1) },
	})
	for i := 0; i < 5; i++ {
		if err := s.Write(asyncRecord(fmt.Sprint("f", i))); err != nil {
			t.Fatal(err)
		}
	}
	c, err := closeWithin(t, s, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c.Failed != 0 || c.Written != 5 || c.Failing || c.LastError != "service refused" {
		t.Fatalf("counts %+v", c)
	}
	if failures.Load() != 1 || recoveries.Load() != 1 {
		t.Fatalf("failure reports %d, recovery reports %d", failures.Load(), recoveries.Load())
	}
	if first := lastFailure.Load().(AsyncCounts); first.Failed != 0 || !first.Failing || first.Queued != 5 {
		t.Fatalf("failure report %+v", first)
	}
	if got := fmt.Sprint(inner.messages()); got != fmt.Sprint([]string{GapMessage, "f0", "f1", "f2", "f3", "f4"}) {
		t.Fatalf("delivered %s", got)
	}
}

func TestAsyncSinkWriteTimeoutRetriesHungDeliveriesUntilTheyReturn(t *testing.T) {
	inner := newStalledSink(true)
	var failures, recoveries atomic.Int32
	var firstErr atomic.Value
	s := NewAsyncSink(inner, AsyncOptions{
		Capacity:       16,
		WriteTimeout:   50 * time.Millisecond,
		InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
		OnFailure: func(err error, _ AsyncCounts) {
			if failures.Add(1) == 1 {
				firstErr.Store(err)
			}
		},
		OnRecovery: func(AsyncCounts) { recoveries.Add(1) },
	})
	for i := range 3 {
		if err := s.Write(asyncRecord(fmt.Sprint("h", i))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "a timed-out delivery and a retry", func() bool { return failures.Load() == 1 && inner.calls.Load() >= 3 })
	if got := <-inner.started; got != "h0" {
		t.Fatalf("first delivery %q", got)
	}
	if err, _ := firstErr.Load().(error); !errors.Is(err, ErrWriteTimeout) {
		t.Fatalf("failure reported %v, want ErrWriteTimeout", err)
	}
	if c := s.Counts(); c.Failed != 0 || c.Written != 0 || c.Queued != 3 || !c.Failing {
		t.Fatalf("counts during the hang %+v", c)
	}
	close(inner.release)
	if err := s.Write(asyncRecord("after")); err != nil {
		t.Fatal(err)
	}
	c, err := closeWithin(t, s, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c.Failed != 0 || c.Written != 4 || c.Failing || failures.Load() != 1 || recoveries.Load() != 1 {
		t.Fatalf("counts %+v, failure reports %d, recovery reports %d", c, failures.Load(), recoveries.Load())
	}
}

func TestAsyncSinkWriteTimeoutNeverCallsAnUncancellableInnerConcurrently(t *testing.T) {
	inner := newStalledSink(false)
	s := NewAsyncSink(plainStalled{inner}, AsyncOptions{Capacity: 16, WriteTimeout: 20 * time.Millisecond,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, OnFailure: quiet})
	for i := range 5 {
		if err := s.Write(asyncRecord(fmt.Sprint("u", i))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "a retry to wait for the hung delivery", func() bool {
		return strings.Contains(s.Counts().LastError, "an earlier delivery has not returned")
	})
	if calls := inner.calls.Load(); calls != 1 {
		t.Fatalf("%d concurrent deliveries while u0 was still running", calls)
	}
	close(inner.release)
	c, err := closeWithin(t, s, 5*time.Second)
	if err != nil || c.Written != 5 || c.Failed != 0 || c.Failing {
		t.Fatalf("close %+v, %v", c, err)
	}
}

func TestAsyncSinkCloseDeadlineWithWriteTimeoutReturnsFinalCounts(t *testing.T) {
	inner := newStalledSink(false)
	s := NewAsyncSink(plainStalled{inner}, AsyncOptions{Capacity: 8, WriteTimeout: time.Minute, OnFailure: quiet})
	for i := range 4 {
		if err := s.Write(asyncRecord(fmt.Sprint("c", i))); err != nil {
			t.Fatal(err)
		}
	}
	<-inner.started
	began := time.Now()
	c, err := closeWithin(t, s, 50*time.Millisecond)
	if !errors.Is(err, ErrCloseDeadline) {
		t.Fatalf("close: %v", err)
	}
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Fatalf("close took %v with a hung, uncancellable delivery", elapsed)
	}
	if c.Accepted != 4 || c.Failed != 1 || c.Abandoned != 3 || c.Written != 0 || c.Queued != 0 {
		t.Fatalf("counts returned by Close %+v", c)
	}
	close(inner.release)
}

func TestAsyncSinkConcurrentWritersAccountForEveryRecord(t *testing.T) {
	inner := &recordingSink{}
	s := NewAsyncSink(inner, AsyncOptions{Capacity: 64})
	var wg sync.WaitGroup
	var dropped atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				switch err := s.Write(asyncRecord(fmt.Sprint(g, "-", i))); {
				case err == nil:
				case errors.Is(err, ErrQueueFull):
					dropped.Add(1)
				default:
					t.Errorf("write: %v", err)
				}
				_ = s.Counts()
			}
		}(g)
	}
	wg.Wait()
	c, err := closeWithin(t, s, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c.Accepted+c.Dropped != 2000 || c.Dropped != uint64(dropped.Load()) || c.Written != c.Accepted || uint64(len(inner.messages())) != c.Written {
		t.Fatalf("counts %+v, observed drops %d, delivered %d", c, dropped.Load(), len(inner.messages()))
	}
}

func TestAsyncSinkUnderSlogHandler(t *testing.T) {
	inner := &recordingSink{}
	s := NewAsyncSink(inner, AsyncOptions{Capacity: 16})
	logger := slog.New(NewHandler(s, &Options{Program: "async-test"}))
	logger.Info("through slog", "k", "v")
	if _, err := closeWithin(t, s, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if len(inner.records) != 1 || inner.records[0].Msg != "through slog" || inner.records[0].Attrs["k"] != "v" || inner.records[0].Identity[0].Program != "async-test" {
		t.Fatalf("records %+v", inner.records)
	}
}

func TestBackoffJitterStaysWithinTwentyPercent(t *testing.T) {
	for range 1000 {
		if d := jittered(time.Second); d < 800*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatalf("jittered wait %v outside 0.8 s to 1.2 s", d)
		}
	}
	if DefaultInitialBackoff != 100*time.Millisecond || DefaultMaxBackoff != 5*time.Second || DefaultWriteTimeout != 5*time.Second || DefaultAsyncCapacity != 1024 {
		t.Fatal("the LOG-S12 defaults changed")
	}
}
