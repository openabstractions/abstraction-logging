package logging

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestResolveDiagnosticsRetainsExplicitStderrOnResolutionFailure(t *testing.T) {
	var stderr, expected bytes.Buffer
	var resolutionContext context.Context
	handler, closeDiagnostics := ResolveDiagnostics("provider", &stderr, func(ctx context.Context, program string) (*Handler, error) {
		resolutionContext = ctx
		deadline, ok := ctx.Deadline()
		if program != "provider" || !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 2*time.Second {
			t.Fatal("resolution program or budget changed", program, deadline)
		}
		return nil, errors.New("runtime unavailable")
	})
	if !errors.Is(resolutionContext.Err(), context.Canceled) {
		t.Fatal("resolution context was not released")
	}
	record := slog.NewRecord(time.Unix(42, 0).UTC(), slog.LevelInfo, "explicit diagnostics", 0)
	record.AddAttrs(slog.String("key", "value"))
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := slog.NewTextHandler(&expected, nil).Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	closeDiagnostics()
	closeDiagnostics()
	if stderr.String() != expected.String() {
		t.Fatalf("diagnostic bytes changed: %q, want %q", stderr.String(), expected.String())
	}
}

func TestResolveDiagnosticsOwnsAdoptedHandlerThroughClose(t *testing.T) {
	var stderr bytes.Buffer
	sink := &recordingSink{}
	adopted := NewHandler(NewAsyncSink(sink, AsyncOptions{}), nil)
	handler, closeDiagnostics := ResolveDiagnostics("provider", &stderr, func(context.Context, string) (*Handler, error) { return adopted, nil })
	if handler != adopted {
		t.Fatal("resolved handler identity changed")
	}
	slog.New(handler).Info("retained record")
	closeDiagnostics()
	closeDiagnostics()
	if got := sink.messages(); len(got) != 1 || got[0] != "retained record" || stderr.Len() != 0 {
		t.Fatal(got, stderr.String())
	}
	if err := handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "after close", 0)); !errors.Is(err, ErrSinkClosed) {
		t.Fatal("adopted handler was not closed", err)
	}
}

func TestResolveDiagnosticsReportsShutdownFailureOnce(t *testing.T) {
	var stderr bytes.Buffer
	sink := newStalledSink(true)
	queue := NewAsyncSink(sink, AsyncOptions{})
	defer func() {
		close(sink.release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := queue.Close(ctx); err != nil {
			t.Fatal("fixture worker did not join", err)
		}
	}()
	handler, closeDiagnostics := ResolveDiagnostics("provider", &stderr, func(context.Context, string) (*Handler, error) { return NewHandler(queue, nil), nil })
	slog.New(handler).Info("pending record")
	<-sink.started
	closeDiagnostics()
	first := stderr.String()
	closeDiagnostics()
	if stderr.String() != first || !strings.HasPrefix(first, "logging shutdown: ") || strings.Count(first, "\n") != 1 || !strings.Contains(first, ErrCloseDeadline.Error()) {
		t.Fatalf("shutdown diagnostic changed or repeated: %q", stderr.String())
	}
}
