package logging

import (
	"context"
	"errors"
	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	"log/slog"
	"testing"
	"time"
)

func TestDefaultHandlerRefusesUnselectedInstallation(t *testing.T) {
	for _, want := range []error{bootstrap.ErrNoTrustedInstallation, bootstrap.ErrAmbiguousInstallation, bootstrap.ErrUnsupportedSelection} {
		handler := Default("trust-test").(*Handler)
		sink := handler.sink.(*resolvedSink)
		called := 0
		sink.selectInstalled = func(ctx context.Context) (bootstrap.Selection, error) {
			called++
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 2*time.Second {
				t.Fatal("missing total waiting budget")
			}
			return bootstrap.Selection{}, want
		}
		err := handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "private record", 0))
		if !errors.Is(err, want) || called != 1 || sink.bound != nil {
			t.Fatalf("unselected default: %v calls=%d bound=%v", err, called, sink.bound)
		}
	}
}

func TestDefaultHandlerCanceledBeforeSelection(t *testing.T) {
	handler := Default("trust-test").(*Handler)
	handler.sink.(*resolvedSink).selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		t.Fatal("selection after cancellation")
		return bootstrap.Selection{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handler.Handle(ctx, slog.NewRecord(time.Now(), slog.LevelInfo, "canceled", 0)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
