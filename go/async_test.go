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

// stalledSink blocks every delivery until release is closed, and reports each
// delivery it started.
type stalledSink struct {
	started chan string
	release chan struct{}
	honour  bool // stop waiting when the delivery context is cancelled
}

func newStalledSink(honour bool) *stalledSink {
	return &stalledSink{started: make(chan string, 64), release: make(chan struct{}), honour: honour}
}

func (s *stalledSink) Write(r Record) error { return s.WriteContext(context.Background(), r) }

func (s *stalledSink) WriteContext(ctx context.Context, r Record) error {
	s.started <- r.Msg
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
	deadline := time.Now().Add(5 * time.Second)
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

func TestAsyncSinkOverflowDropsNewestCountedAndReported(t *testing.T) {
	inner := newStalledSink(true)
	var overflows []AsyncCounts
	var mu sync.Mutex
	s := NewAsyncSink(inner, AsyncOptions{Capacity: 2, OnOverflow: func(c AsyncCounts) {
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
	if c.Accepted != 3 || c.Dropped != 3 || !c.Overflowing || c.Queued != 2 {
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
	if s.Counts().Overflowing {
		t.Fatal("overflow state not reset by an accepted record")
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
	s := NewAsyncSink(inner, AsyncOptions{Capacity: 8})
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
	eventually(t, "worker to finish", func() bool {
		c := s.Counts()
		return c.Failed+c.Abandoned+c.Written == 5 && c.Queued == 0
	})
	c := s.Counts()
	if c.Failed != 1 || c.Abandoned != 4 || c.Written != 0 {
		t.Fatalf("counts %+v", c)
	}
	select {
	case m := <-inner.started:
		t.Fatalf("delivery %q started after the close deadline", m)
	default:
	}
}

func TestAsyncSinkCloseReturnsAtDeadlineWhenInnerCannotBeCancelled(t *testing.T) {
	inner := newStalledSink(false)
	s := NewAsyncSink(plainStalled{inner}, AsyncOptions{Capacity: 4})
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

func TestAsyncSinkFailureAndRecoveryReportedOncePerTransition(t *testing.T) {
	inner := &recordingSink{}
	inner.fail.Store(3)
	var failures, recoveries atomic.Int32
	var lastFailure atomic.Value
	s := NewAsyncSink(inner, AsyncOptions{
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
	if c.Failed != 3 || c.Written != 2 || c.Failing || c.LastError != "service refused" {
		t.Fatalf("counts %+v", c)
	}
	if failures.Load() != 1 || recoveries.Load() != 1 {
		t.Fatalf("failure reports %d, recovery reports %d", failures.Load(), recoveries.Load())
	}
	if first := lastFailure.Load().(AsyncCounts); first.Failed != 1 || !first.Failing {
		t.Fatalf("failure report %+v", first)
	}
}

func TestAsyncSinkWriteTimeoutCancelsHungDeliveriesAndKeepsDelivering(t *testing.T) {
	inner := newStalledSink(true)
	var failures, recoveries atomic.Int32
	var firstErr atomic.Value
	s := NewAsyncSink(inner, AsyncOptions{
		Capacity:     16,
		WriteTimeout: 50 * time.Millisecond,
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
	eventually(t, "three timed-out deliveries", func() bool { return s.Counts().Failed == 3 })
	for i := range 3 {
		select {
		case got := <-inner.started:
			if got != fmt.Sprint("h", i) {
				t.Fatalf("attempt %d delivered %q", i, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("attempt %d never started; counts %+v", i, s.Counts())
		}
	}
	if err, _ := firstErr.Load().(error); !errors.Is(err, ErrWriteTimeout) {
		t.Fatalf("failure reported %v, want ErrWriteTimeout", err)
	}
	close(inner.release)
	if err := s.Write(asyncRecord("after")); err != nil {
		t.Fatal(err)
	}
	c, err := closeWithin(t, s, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c.Failed != 3 || c.Written != 1 || c.Failing || failures.Load() != 1 || recoveries.Load() != 1 {
		t.Fatalf("counts %+v, failure reports %d, recovery reports %d", c, failures.Load(), recoveries.Load())
	}
}

func TestAsyncSinkWriteTimeoutNeverCallsAnUncancellableInnerConcurrently(t *testing.T) {
	inner := newStalledSink(false)
	s := NewAsyncSink(plainStalled{inner}, AsyncOptions{Capacity: 16, WriteTimeout: 50 * time.Millisecond})
	for i := range 5 {
		if err := s.Write(asyncRecord(fmt.Sprint("u", i))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "every record to fail", func() bool { c := s.Counts(); return c.Failed == 5 && c.Queued == 0 })
	if got := <-inner.started; got != "u0" {
		t.Fatalf("first delivery %q", got)
	}
	select {
	case m := <-inner.started:
		t.Fatalf("delivery %q started while u0 was still running", m)
	default:
	}
	if c := s.Counts(); !strings.Contains(c.LastError, "an earlier delivery has not returned") {
		t.Fatalf("last error %q", c.LastError)
	}
	close(inner.release)
	delivered := false
	for i := 0; i < 200 && !delivered; i++ {
		msg := fmt.Sprint("later", i)
		if err := s.Write(asyncRecord(msg)); err != nil {
			t.Fatal(err)
		}
		eventually(t, msg+" to settle", func() bool { c := s.Counts(); return c.Written+c.Failed == c.Accepted })
		delivered = s.Counts().Written == 1
	}
	if !delivered {
		t.Fatal("no record was delivered after the hung delivery returned")
	}
	c, err := closeWithin(t, s, 5*time.Second)
	if err != nil || c.Written != 1 || c.Written+c.Failed != c.Accepted {
		t.Fatalf("close %+v, %v", c, err)
	}
}

func TestAsyncSinkCloseDeadlineWithWriteTimeoutReturnsFinalCounts(t *testing.T) {
	inner := newStalledSink(false)
	s := NewAsyncSink(plainStalled{inner}, AsyncOptions{Capacity: 8, WriteTimeout: time.Minute})
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
