package logging

import (
	"context"
	"errors"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func waitObservers(t *testing.T, s *FileSink, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		actual := s.historyWaiters
		s.mu.Unlock()
		if actual == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiters %d want %d", actual, n)
		}
		runtime.Gosched()
	}
}
func TestObservationNotifyCapacityCancelAndShutdown(t *testing.T) {
	s, e := OpenFileSink(filepath.Join(t.TempDir(), "history"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan wire.Page, MaxHistoryWaiters)
	errs := make(chan error, MaxHistoryWaiters)
	for i := 0; i < MaxHistoryWaiters; i++ {
		go func() { p, e := s.ObserveContext(ctx, "", 1, 65536, 30000); results <- p; errs <- e }()
	}
	waitObservers(t, s, MaxHistoryWaiters)
	p, e := s.ObserveContext(ctx, "", 1, 65536, 30000)
	if e != nil || p.Outcome != wire.PageOutcomeUnavailable {
		t.Fatalf("capacity %+v %v", p, e)
	}
	if e = s.Write(Record{Schema: 1, Time: Timestamp{time.Now().UTC()}, Msg: "wake"}); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < MaxHistoryWaiters; i++ {
		select {
		case p = <-results:
			if p.Outcome != wire.PageOutcomePage || len(p.Records) != 1 || p.Records[0].Msg != "wake" {
				t.Fatalf("notify %+v", p)
			}
			if e = <-errs; e != nil {
				t.Fatal(e)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("write did not notify")
		}
	}
	waitObservers(t, s, 0)
	cursor := p.Next
	go func() { _, e := s.ObserveContext(ctx, cursor, 1, 65536, 30000); errs <- e }()
	waitObservers(t, s, 1)
	cancel()
	if e = <-errs; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	waitObservers(t, s, 0)
	go func() {
		p, e := s.ObserveContext(context.Background(), cursor, 1, 65536, 30000)
		results <- p
		errs <- e
	}()
	waitObservers(t, s, 1)
	s.Close()
	select {
	case p = <-results:
		if p.Outcome != wire.PageOutcomeUnavailable {
			t.Fatalf("closed %+v", p)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not wake")
	}
	waitObservers(t, s, 0)
}
func TestObservationExpirySlowReaderAndRestartGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history")
	s, e := OpenFileSink(path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p, e := s.ObserveContext(context.Background(), "", 1, 65536, 1)
	if e != nil || p.Outcome != wire.PageOutcomePage || !p.AtEnd || len(p.Records) != 0 {
		t.Fatalf("expiry %+v %v", p, e)
	}
	cursor := p.Next
	for i := 0; i < 5; i++ {
		if e = s.Write(Record{Schema: 1, Time: Timestamp{time.Now().UTC()}, Msg: "later"}); e != nil {
			t.Fatal(e)
		}
	}
	p, e = s.ObserveContext(context.Background(), cursor, 1, 65536, 30000)
	if e != nil || len(p.Records) != 1 || p.AtEnd {
		t.Fatalf("bounded slow reader %+v %v", p, e)
	}
	s.Close()
	s, e = OpenFileSink(path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p, e = s.ObserveContext(context.Background(), cursor, 1, 65536, 30000)
	if e != nil || p.Outcome != wire.PageOutcomeGap || p.Next != cursor {
		t.Fatalf("restart %+v %v", p, e)
	}
}

// The old generation remains signalled even when a write wins before the waiter selects.
func TestObservationCapturedGenerationSurvivesEarlyWrite(t *testing.T) {
	s, e := OpenFileSink(filepath.Join(t.TempDir(), "history"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan wire.Page, 1)
	go func() { p, _ := s.ObserveContext(ctx, "", 1, 65536, 30000); done <- p }()
	waitObservers(t, s, 1)
	s.mu.Lock()
	generation := s.historyChanged
	s.mu.Unlock()
	if e = s.Write(Record{Schema: 1, Time: Timestamp{time.Now().UTC()}, Msg: "early"}); e != nil {
		t.Fatal(e)
	}
	select {
	case <-generation:
	default:
		t.Fatal("captured generation lost notification")
	}
	select {
	case p := <-done:
		if len(p.Records) != 1 {
			t.Fatalf("reread %+v", p)
		}
	case <-time.After(time.Second):
		t.Fatal("notification lost")
	}
}

func TestObservationExpiryRereadsWithoutNotification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history")
	s, e := OpenFileSink(path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	result := make(chan wire.Page, 1)
	go func() { p, _ := s.ObserveContext(context.Background(), "", 1, 65536, 100); result <- p }()
	waitObservers(t, s, 1)
	// External writers have no event promise; the expiry path still rereads once.
	f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	record := Record{Schema: 1, Time: Timestamp{time.Now().UTC()}, Msg: "expiry"}
	encoded, e := record.Encode()
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.Write(encoded)
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	select {
	case p := <-result:
		if len(p.Records) != 1 || p.Records[0].Msg != "expiry" {
			t.Fatalf("expiry did not reread %+v", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expiry blocked")
	}
}
