package logging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
// facade. With no runtime installed, Handle queues the record and returns;
// the eager Check and Close report the facade's resolution error, and the
// record reaches no stderr, file or discard sink in its place.
func TestAdoptedSinkWithNoRuntimeFailsWithTheResolutionError(t *testing.T) {
	handler := Default("no-runtime")
	var selections atomic.Int32
	handler.sink.(*adoptedSink).resolved.selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		selections.Add(1)
		return bootstrap.Selection{}, bootstrap.ErrNoTrustedInstallation
	}
	var err error
	stderr := stderrOf(t, func() {
		err = handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelError, "startup record", 0))
	})
	if err != nil {
		t.Fatalf("Handle with no runtime returned %v, want the record queued", err)
	}
	if strings.Contains(stderr, "startup record") {
		t.Fatalf("the record was written to stderr: %q", stderr)
	}
	// The eager form reports the resolution error.
	var refused *ResolutionError
	check := handler.Check(context.Background())
	if !errors.As(check, &refused) {
		t.Fatalf("Check with no runtime returned %T %v, want the facade's *ResolutionError", check, check)
	}
	if refused.Status != resolution.RuntimeUnavailable || refused.Capability != "abstraction.logging" ||
		refused.Contract != "abstraction.logging/sink@1" || refused.LookedFor != "the installed runtime" {
		t.Fatalf("resolution error %+v", refused)
	}
	if !errors.Is(check, bootstrap.ErrNoTrustedInstallation) || selections.Load() < 1 {
		t.Fatalf("cause %v after %d selections, want the selection failure", check, selections.Load())
	}
	if !strings.HasPrefix(check.Error(), "service resolution: runtime_unavailable: abstraction.logging/sink@1") {
		t.Fatalf("message %q", check)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts, err := handler.Close(ctx)
	if !errors.As(err, &refused) || refused.Status != resolution.RuntimeUnavailable || counts.Failed != 1 || counts.Written != 0 {
		t.Fatalf("Close with no runtime: %+v, %v", counts, err)
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

// The homepage quickstart with no runtime: slog.Logger discards the
// resolution error Handle returns, and Close reports it with the refused
// record counted [LOG-S11]. This runs the real installed-runtime selection,
// which finds none in a test binary.
func TestQuickstartCloseWithNoRuntimeReturnsTheResolutionError(t *testing.T) {
	h := NewDefaultHandler(&Options{Program: "my-app"})
	logger := slog.New(h)
	stderr := stderrOf(t, func() { logger.Info("worker started", "component", "indexer") })
	if strings.Contains(stderr, "worker started") {
		t.Fatalf("logging wrote the record to stderr: %q", stderr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts, err := h.Close(ctx)
	var refused *ResolutionError
	if !errors.As(err, &refused) || refused.Status != resolution.RuntimeUnavailable ||
		refused.Contract != "abstraction.logging/sink@1" {
		t.Fatalf("Close with no runtime returned %v, want the facade's runtime_unavailable resolution error", err)
	}
	if counts.Written != 0 || counts.Failed != 1 || counts.LastError != refused.Error() {
		t.Fatalf("counts %+v, want one failed record and the resolution error", counts)
	}
	if !strings.Contains(err.Error(), "1 records not delivered") {
		t.Fatalf("message %q", err)
	}
}

// A handler that logged nothing has nothing to report on Close.
func TestCloseWithNothingLoggedReturnsNil(t *testing.T) {
	counts, err := NewDefaultHandler(&Options{Program: "idle"}).Close(context.Background())
	if err != nil || counts != (AsyncCounts{}) {
		t.Fatalf("Close of an idle handler: %+v, %v", counts, err)
	}
}

// A handler closed before its first record stays closed: a later record gets
// ErrSinkClosed and starts no delivery goroutine. A provider that fails
// construction closes its diagnostics before it ever logged.
func TestAdoptedSinkClosedBeforeFirstRecordRefusesLaterRecords(t *testing.T) {
	handler := Default("closed-early")
	sink := handler.sink.(*adoptedSink)
	sink.resolved.selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		return bootstrap.Selection{}, bootstrap.ErrNoTrustedInstallation
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := handler.Close(ctx); err != nil {
		t.Fatalf("Close with nothing queued returned %v", err)
	}
	if err := handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "after close", 0)); !errors.Is(err, ErrSinkClosed) {
		t.Fatalf("Handle after Close returned %v, want ErrSinkClosed", err)
	}
	if sink.async() != nil {
		t.Fatal("a record after Close started a delivery goroutine")
	}
}
