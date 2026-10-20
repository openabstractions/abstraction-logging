package logging

import (
	"context"
	"errors"
	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestDefaultHandlerRefusesUnselectedInstallation(t *testing.T) {
	for _, want := range []error{bootstrap.ErrNoTrustedInstallation, bootstrap.ErrAmbiguousInstallation, bootstrap.ErrUnsupportedSelection} {
		handler := Default("trust-test")
		sink := handler.sink.(*adoptedSink).resolved
		var called atomic.Int32
		sink.selectInstalled = func(ctx context.Context) (bootstrap.Selection, error) {
			called.Add(1)
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 5*time.Second {
				t.Error("missing total waiting budget")
			}
			return bootstrap.Selection{}, want
		}
		if err := handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "private record", 0)); err != nil {
			t.Fatalf("Handle refused the record: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		counts, err := handler.Close(ctx)
		cancel()
		if !errors.Is(err, want) || called.Load() < 1 || sink.bound != nil || counts.Written != 0 || counts.Failed != 1 {
			t.Fatalf("unselected default: %v calls=%d bound=%v counts=%+v", err, called.Load(), sink.bound, counts)
		}
	}
}

func TestDefaultHandlerCanceledBeforeSelection(t *testing.T) {
	handler := Default("trust-test")
	handler.sink.(*adoptedSink).resolved.selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		t.Fatal("selection after cancellation")
		return bootstrap.Selection{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handler.Handle(ctx, slog.NewRecord(time.Now(), slog.LevelInfo, "canceled", 0)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
