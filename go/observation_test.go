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
	if e = s.Write(Record{Schema: 1, Time: Timestamp{Time: time.Now().UTC()}, Msg: "wake"}); e != nil {
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
		if e = s.Write(Record{Schema: 1, Time: Timestamp{Time: time.Now().UTC()}, Msg: "later"}); e != nil {
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
	if e = s.Write(Record{Schema: 1, Time: Timestamp{Time: time.Now().UTC()}, Msg: "early"}); e != nil {
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
	record := Record{Schema: 1, Time: Timestamp{Time: time.Now().UTC()}, Msg: "expiry"}
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

// The cursor word end begins at the current end of retained history: a read
// returns no records and the continuation there, an observation returns only
// the records appended after it began, and a refusal keeps the word.
func TestHistoryEndCursorBeginsAtTheCurrentEnd(t *testing.T) {
	end := wire.HistoryCursorWords[0]
	if len(wire.HistoryCursorWords) != 1 || end != EndCursor {
		t.Fatalf("cursor words %q, EndCursor %q", wire.HistoryCursorWords, EndCursor)
	}
	s, e := OpenFileSink(filepath.Join(t.TempDir(), "history"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	write := func(msg string) {
		t.Helper()
		if err := s.Write(Record{Schema: 1, Time: Timestamp{Time: time.Now().UTC()}, Msg: msg}); err != nil {
			t.Fatal(err)
		}
	}
	empty, e := s.Read(end, 1, 1)
	if e != nil || empty.Outcome != wire.PageOutcomePage || len(empty.Records) != 0 || !empty.AtEnd || empty.Next == "" || empty.Next == end {
		t.Fatalf("end of empty history: %+v %v", empty, e)
	}
	write("old-1")
	write("old-2")
	p, e := s.Read(end, 256, 65536)
	if e != nil || p.Outcome != wire.PageOutcomePage || len(p.Records) != 0 || !p.AtEnd || p.Next == end {
		t.Fatalf("read at end: %+v %v", p, e)
	}
	write("new-1")
	next, _ := s.Read(p.Next, 256, 65536)
	if len(next.Records) != 1 || next.Records[0].Msg != "new-1" {
		t.Fatalf("after the end continuation: %+v", next)
	}
	result := make(chan wire.Page, 1)
	go func() { p, _ := s.ObserveContext(context.Background(), end, 256, 65536, 5000); result <- p }()
	waitObservers(t, s, 1)
	write("new-2")
	select {
	case observed := <-result:
		if observed.Outcome != wire.PageOutcomePage || len(observed.Records) != 1 || observed.Records[0].Msg != "new-2" || observed.Next == end {
			t.Fatalf("observe from end: %+v", observed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("observation from end was not notified")
	}
	expired, _ := s.ObserveContext(context.Background(), end, 256, 65536, 10)
	if expired.Outcome != wire.PageOutcomePage || len(expired.Records) != 0 || !expired.AtEnd || expired.Next == end {
		t.Fatalf("expired observation from end: %+v", expired)
	}
	for _, limits := range [][2]int64{{0, 1}, {1, 0}} {
		refused, _ := s.Read(end, limits[0], limits[1])
		if refused.Outcome != wire.PageOutcomeInvalidRequest || refused.Next != end || refused.AtEnd {
			t.Fatalf("refusal with end: %+v", refused)
		}
	}
}
