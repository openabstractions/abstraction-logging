package logging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	"github.com/openabstractions/abstraction-facade/go-core/resolution"
)

// VISION.md 2026-09-16, "An adopted capability with no runtime fails, like a
// graphics API with no driver": when resolution fails, the operation fails
// with the facade's resolution error, visibly, and the application does not
// substitute its own implementation. "A logging seam that quietly writes to
// stderr instead is the same defect." These tests state that ruling for the
// sinks this package builds [LOG-S11], and the one fallback that remains: a
// local provider the application selects by name [LOG-S5].

type sinkFunc func(Record) error

func (f sinkFunc) Write(r Record) error { return f(r) }

// stderrOf runs f with os.Stderr redirected and returns what f wrote there.
func stderrOf(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stderr
	os.Stderr = w
	func() {
		defer func() { os.Stderr = saved }()
		f()
	}()
	w.Close()
	out, _ := io.ReadAll(r)
	r.Close()
	return string(out)
}

// The adopted sink: Default resolves abstraction.logging/sink@1 through the
// facade. With no runtime installed, a record is the facade's resolution error
// and reaches no stderr, file or discard sink in its place.
func TestAdoptedSinkWithNoRuntimeFailsWithTheResolutionError(t *testing.T) {
	handler := Default("no-runtime").(*Handler)
	selections := 0
	handler.sink.(*adoptedSink).resolved.selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		selections++
		return bootstrap.Selection{}, bootstrap.ErrNoTrustedInstallation
	}
	var err error
	stderr := stderrOf(t, func() {
		err = handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelError, "startup", 0))
	})
	var refused *ResolutionError
	if !errors.As(err, &refused) {
		t.Fatalf("Handle with no runtime returned %T %v, want the facade's *ResolutionError", err, err)
	}
	if refused.Status != resolution.RuntimeUnavailable || refused.Capability != "abstraction.logging" ||
		refused.Contract != "abstraction.logging/sink@1" || refused.LookedFor != "the installed runtime" {
		t.Fatalf("resolution error %+v", refused)
	}
	if !errors.Is(err, bootstrap.ErrNoTrustedInstallation) || selections != 1 {
		t.Fatalf("cause %v after %d selections, want the selection failure after one", err, selections)
	}
	if !strings.HasPrefix(err.Error(), "service resolution: runtime_unavailable: abstraction.logging/sink@1") {
		t.Fatalf("message %q", err)
	}
	if stderr != "" {
		t.Fatalf("the record was written to stderr instead of failing: %q", stderr)
	}
	// The eager form reports the same error before any record is logged.
	if check := handler.Check(context.Background()); !errors.As(check, &refused) || refused.Status != resolution.RuntimeUnavailable {
		t.Fatalf("Check with no runtime: %v", check)
	}
}

// The socket sink this package addresses by path fails the write when nobody
// listens. It keeps no fallback of its own.
func TestServiceSinkWithNobodyListeningFailsTheWrite(t *testing.T) {
	sink := NewServiceSink(filepath.Join(t.TempDir(), "nothing.sock"))
	defer sink.Close()
	var err error
	stderr := stderrOf(t, func() { err = sink.Write(Record{Schema: SchemaVersion, Msg: "x"}) })
	if err == nil {
		t.Fatal("a write to a service nobody listens on succeeded")
	}
	if stderr != "" {
		t.Fatalf("the record was written to stderr instead of failing: %q", stderr)
	}
}

// Degrading to a lower rung is the environment chain's LocalFallbackSink, a
// local provider the application names: LegacyAuto builds it, or the
// application constructs it.
func TestLocalFallbackIsAProviderTheApplicationSelects(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvService, filepath.Join(dir, "deaf.sock"))
	t.Setenv(EnvSink, filepath.Join(dir, "local.jsonl"))
	fallback, ok := LegacyAuto("p").(*LocalFallbackSink)
	if !ok {
		t.Fatal("LegacyAuto with a service socket configured is not a LocalFallbackSink")
	}
	file := fallback.Local.(*FileSink)
	defer file.Close()
	defer fallback.Close()
	if err := fallback.Write(Record{Schema: SchemaVersion, Time: At(time.Now()), Msg: "kept locally"}); err != nil {
		t.Fatalf("the selected local fallback failed: %v", err)
	}
	b, err := os.ReadFile(file.Path())
	if err != nil || !strings.Contains(string(b), "kept locally") {
		t.Fatalf("local rung got %q, %v", b, err)
	}
	if SeparationOf(fallback) == SeparationPeer {
		t.Fatal("a sink whose records can land in a file claims peer separation")
	}

	var local []Record
	explicit := NewLocalFallbackSink(NewServiceSink(filepath.Join(dir, "nobody.sock")),
		sinkFunc(func(r Record) error { local = append(local, r); return nil }))
	defer explicit.Close()
	if err := explicit.Write(Record{Schema: SchemaVersion, Msg: "x"}); err != nil || len(local) != 1 {
		t.Fatalf("explicit local fallback: %v, %d records", err, len(local))
	}
	if NewLocalFallbackSink(NewServiceSink(filepath.Join(dir, "nobody.sock")), nil).Local != Sink(DiscardSink{}) {
		t.Fatal("a nil local rung is not the discarding sink")
	}
}
