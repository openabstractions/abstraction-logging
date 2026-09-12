package service

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	"github.com/openabstractions/abstraction-logging/go/client"
)

type blockedSink struct{ entered, release chan struct{} }

func (s blockedSink) Write(logging.Record) error { close(s.entered); <-s.release; return nil }

func TestAdmissionStopsBeforeProviderDrains(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("Program proof unavailable; existing service test verifies refusal")
	}
	sink := blockedSink{make(chan struct{}), make(chan struct{})}
	endpoint := listen.Endpoint(fmt.Sprintf("drain-%d", time.Now().UnixNano()))
	h, err := Listen(endpoint, sink)
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	h.OnStopped = func() { close(stopped) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	callDone := make(chan error, 1)
	go func() { done <- h.Serve(ctx) }()
	go func() { callDone <- client.New(endpoint).Log(1, "blocked", nil) }()
	defer func() {
		cancel()
		h.Close()
		close(sink.release)
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("host failed to drain")
		}
		select {
		case <-callDone:
		case <-time.After(3 * time.Second):
			t.Error("client failed to finish")
		}
	}()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("write never reached provider")
	}
	h.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("readiness waits for blocked provider")
	}
	select {
	case <-done:
		t.Fatal("Serve returned before provider drained")
	default:
	}
}
