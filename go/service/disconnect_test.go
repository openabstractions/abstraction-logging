package service

import (
	"context"
	"errors"
	logging "github.com/openabstractions/abstraction-logging/go"
	client "github.com/openabstractions/abstraction-logging/go/client"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestObservationDisconnectReleasesSlot(t *testing.T) {
	f, e := logging.OpenFileSink(filepath.Join(t.TempDir(), "history"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	s := &observingSink{FileSink: f, entered: make(chan struct{})}
	h, endpoint, _ := observationHost(t, s, false)
	c, e := client.NewObserver(endpoint).WithTimeout(35 * time.Second)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.ObserveContext(ctx, "", 1, 65536, 30000); done <- e }()
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("provider wait not entered")
	}
	for held := time.Now().Add(3 * time.Second); s.HistoryGate().Waiting() != 1 && time.Now().Before(held); {
		runtime.Gosched()
	}
	if s.HistoryGate().Waiting() != 1 {
		t.Fatal("missing observation slot")
	}
	cancel()
	select {
	case e = <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not cancel")
	}
	deadline := time.Now().Add(2 * time.Second)
	for s.HistoryGate().Waiting() != 0 || len(h.calls) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("disconnected caller retained observer=%d calls=%d", s.HistoryGate().Waiting(), len(h.calls))
		}
		runtime.Gosched()
	}
	// Cancellation concerns observation only: writing and a fresh observation remain usable.
	if e = client.New(endpoint).Log(1, "after canceled wait", nil); e != nil {
		t.Fatal(e)
	}
	p, e := c.ObserveContext(context.Background(), "", 1, 65536, 1000)
	if e != nil || len(p.Records) != 1 || p.Records[0].Msg != "after canceled wait" {
		t.Fatalf("fresh call %+v %v", p, e)
	}
}
