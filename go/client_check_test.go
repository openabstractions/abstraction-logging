package logging

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
)

func TestCheckReportsTheSelectionError(t *testing.T) {
	handler := Default("check-test").(*Handler)
	calls := 0
	handler.sink.(*resolvedSink).selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		calls++
		return bootstrap.Selection{}, bootstrap.ErrNoTrustedInstallation
	}
	if err := handler.Check(context.Background()); !errors.Is(err, bootstrap.ErrNoTrustedInstallation) || calls != 1 {
		t.Fatalf("check: %v, selections %d", err, calls)
	}
}

func TestCheckHasNothingToResolveForOtherSinks(t *testing.T) {
	sink, err := OpenFileSink(filepath.Join(t.TempDir(), "check.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	for _, h := range []*Handler{NewHandler(sink, nil), NewHandler(DiscardSink{}, nil)} {
		if err := h.Check(context.Background()); err != nil {
			t.Fatalf("check on %T: %v", h.sink, err)
		}
	}
}

func TestResolveReportsWithoutAnInstalledRuntime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if h, err := Resolve(ctx, "resolve-test"); err == nil || h != nil {
		t.Fatalf("resolve with a cancelled context: %v %v", h, err)
	}
}
