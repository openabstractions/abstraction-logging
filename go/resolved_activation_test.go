//go:build windows || linux

package logging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	facade "github.com/openabstractions/abstraction-facade/go-core/go/abstraction/facade"
	"github.com/openabstractions/abstraction-facade/go-core/resolution"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
)

func absentEndpoint(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		return fmt.Sprintf(`\\.\pipe\oa-logging-activation-%d-%d`, os.Getpid(), time.Now().UnixNano())
	}
	dir, err := os.MkdirTemp("", "oa-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "runtime.sock")
}

// stoppedInstallation is the Default handler with an installation selected and
// no runtime listening at its endpoint.
func stoppedInstallation(t *testing.T) (*Handler, *resolvedSink, bootstrap.Selection) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	principal, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	selected := bootstrap.Selection{Endpoint: absentEndpoint(t), Server: listen.ServerExpectation{Principal: identity.User{Kind: "windows", SID: principal.Uid}, Program: exe}}
	if runtime.GOOS == "linux" {
		selected.Server.Principal = identity.User{Kind: "posix", UID: os.Geteuid(), GID: -1}
	}
	handler := Default("activation-test")
	sink := handler.sink.(*adoptedSink).resolved
	sink.selectInstalled = func(context.Context) (bootstrap.Selection, error) { return selected, nil }
	return handler, sink, selected
}

type sinkResolver struct{}

func (sinkResolver) Resolve(request facade.ResolveRequest) (facade.ResolveResult, error) {
	return facade.ResolveResult{Status: facade.ResolutionStatusResolved, Reference: &facade.ServiceReference{Provider: "fixture", Capability: request.Capability,
		Contract: request.Contracts[0], Scope: facade.ScopeLocal, Transport: resolution.LocalTransport, Endpoint: "activated-provider"}}, nil
}

func (r sinkResolver) ResolveOnMachine(request facade.ResolveRequest, bindingName string) (facade.ResolveResult, error) {
	if bindingName != "" {
		return facade.ResolveResult{}, errors.New("fixture resolver has no paired machines")
	}
	return r.Resolve(request)
}

// serveResolver answers one resolver exchange at endpoint, as an activated
// runtime would.
func serveResolver(t *testing.T, endpoint string) <-chan error {
	t.Helper()
	listener, err := listen.Listen(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		defer listener.Close()
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		call, err := listen.ReceiveFramed(ctx, conn, listen.Program, 1<<20)
		if err != nil {
			done <- err
			return
		}
		defer call.Close()
		dispatcher := facade.ResolverDispatcher{Handler: sinkResolver{}}
		reply, err := dispatcher.ExchangeFrame(call.Frame)
		if err == nil {
			err = call.Reply(reply)
		}
		done <- err
	}()
	return done
}

func TestDefaultHandlerActivatesAStoppedInstallationOnce(t *testing.T) {
	handler, sink, selected := stoppedInstallation(t)
	var activations atomic.Int32
	var served <-chan error
	sink.activate = func(ctx context.Context, selection bootstrap.Selection) error {
		activations.Add(1)
		if selection.Server.Program != selected.Server.Program || selection.Endpoint != selected.Endpoint {
			t.Errorf("activated %+v, want the selected installation %+v", selection, selected)
		}
		// A record's 2 s write bound does not shorten the activation budget.
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) <= 2*time.Second {
			t.Errorf("activation budget %v %v, want the default budget", deadline, ok)
		}
		served = serveResolver(t, selected.Endpoint)
		return nil
	}
	if err := handler.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if activations.Load() != 1 || sink.bound == nil {
		t.Fatalf("activations %d bound %v", activations.Load(), sink.bound)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}

	handler, sink, selected = stoppedInstallation(t)
	sink.activate = func(ctx context.Context, selection bootstrap.Selection) error {
		// The record's binding activates once. The queue's later rebinds find
		// the fixture resolver gone and are refused here, so nothing else serves.
		if activations.Add(1) != 2 {
			return errors.New("fixture activates once")
		}
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) <= 2*time.Second {
			t.Errorf("record activation budget %v %v, want the default budget", deadline, ok)
		}
		served = serveResolver(t, selected.Endpoint)
		return nil
	}
	// The resolved provider endpoint is a fixture nobody serves. The record is
	// queued at once, and the delivery goroutine's binding needed exactly one
	// activation.
	handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "after activation", 0))
	adopted := handler.sink.(*adoptedSink)
	queue := adopted.async()
	if queue == nil {
		t.Fatal("the record was not queued")
	}
	for deadline := time.Now().Add(5 * time.Second); !sink.everBound.Load(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("never bound after %d activations", activations.Load())
		}
	}
	// The write to the unserved provider fails, and its rebind may start the
	// runtime once more; that start fails and is remembered.
	if n := activations.Load(); n < 2 || n > 3 {
		t.Fatalf("record activations %d", n)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	closing, cancelClose := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelClose()
	queue.Close(closing)
}

func TestDefaultHandlerReportsAnUpgradeInProgress(t *testing.T) {
	handler, sink, selected := stoppedInstallation(t)
	calls := 0
	sink.activate = func(context.Context, bootstrap.Selection) error {
		calls++
		return fmt.Errorf("%w: start refused: an upgrade of this installation is in progress", bootstrap.ErrUpgradeInProgress)
	}
	err := handler.Check(context.Background())
	var refused *ResolutionError
	if !errors.As(err, &refused) || refused.Status != resolution.UpgradeInProgress || !errors.Is(err, bootstrap.ErrUpgradeInProgress) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	if refused.LookedFor != "the installed runtime at "+selected.Endpoint || refused.Capability != "abstraction.logging" {
		t.Fatalf("resolution error %+v", refused)
	}
}

func TestDefaultHandlerReportsARefusedActivationAsRuntimeUnavailable(t *testing.T) {
	handler, sink, _ := stoppedInstallation(t)
	calls := 0
	sink.activate = func(context.Context, bootstrap.Selection) error {
		calls++
		return fmt.Errorf("%w: the caller's token is elevated", bootstrap.ErrActivationRefused)
	}
	err := handler.Check(context.Background())
	var refused *ResolutionError
	if !errors.As(err, &refused) || refused.Status != resolution.RuntimeUnavailable || !errors.Is(err, bootstrap.ErrActivationRefused) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestDefaultHandlerWithNoInstallationActivatesNothing(t *testing.T) {
	handler := Default("activation-test")
	sink := handler.sink.(*adoptedSink).resolved
	sink.selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		return bootstrap.Selection{}, bootstrap.ErrNoTrustedInstallation
	}
	sink.activate = func(context.Context, bootstrap.Selection) error {
		t.Fatal("activated with no installation")
		return nil
	}
	err := handler.Check(context.Background())
	var refused *ResolutionError
	if !errors.As(err, &refused) || refused.Status != resolution.RuntimeUnavailable || !errors.Is(err, bootstrap.ErrNoTrustedInstallation) {
		t.Fatal(err)
	}
}

// An explicit endpoint is the caller's choice; an absent one is reported as
// is. Had it activated, bootstrap.ActivateInstalled would refuse this test
// binary, which is no openabstractions.exe, and join ErrActivationRefused.
func TestExplicitEndpointIsNeverActivated(t *testing.T) {
	_, _, selected := stoppedInstallation(t)
	sink := resolvedSinkFor(selected.Endpoint, selected.Server)
	_, err := sink.binding(context.Background())
	var refused *ResolutionError
	if !errors.As(err, &refused) || refused.Status != resolution.RuntimeUnavailable {
		t.Fatal(err)
	}
	if errors.Is(err, bootstrap.ErrActivationRefused) || errors.Is(err, bootstrap.ErrActivationUnsupported) {
		t.Fatalf("explicit endpoint activated: %v", err)
	}
}

// A refused or untrusted server is reported as it is: only an absent endpoint
// activates.
func TestPresentEndpointIsNeverActivated(t *testing.T) {
	handler, sink, selected := stoppedInstallation(t)
	selected.Server.Program = filepath.Join(filepath.Dir(selected.Server.Program), "another-runtime")
	sink.selectInstalled = func(context.Context) (bootstrap.Selection, error) { return selected, nil }
	listener, err := listen.Listen(selected.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		if conn, err := listener.Accept(); err == nil {
			conn.Close()
		}
	}()
	sink.activate = func(context.Context, bootstrap.Selection) error {
		t.Fatal("activated a present endpoint")
		return nil
	}
	if err := handler.Check(context.Background()); err == nil {
		t.Fatal("an untrusted server resolved")
	}
}

// While the installed runtime cannot start, a log statement never waits on it.
// The record is queued at once, the start runs once in the background and is
// remembered when it fails, and Close reports the failure with every queued
// record counted.
func TestDefaultHandlerNeverBlocksWhileTheRuntimeCannotStart(t *testing.T) {
	_, sink, _ := stoppedInstallation(t)
	var activations atomic.Int32
	started := make(chan struct{})
	sink.activate = func(ctx context.Context, _ bootstrap.Selection) error {
		if activations.Add(1) == 1 {
			close(started)
		}
		// A start that takes longer than a delivery's write timeout, then fails.
		select {
		case <-time.After(6 * time.Second):
		case <-ctx.Done():
		}
		return fmt.Errorf("%w: the runtime exited at start", bootstrap.ErrActivationRefused)
	}
	var failures atomic.Int32
	h := NewDefaultHandler(&Options{Program: "blocked-start", OnFailure: func(error, AsyncCounts) { failures.Add(1) }})
	h.sink.(*adoptedSink).resolved = sink
	logger := slog.New(h)
	began := time.Now()
	for i := range 200 {
		logger.Info("record while the runtime cannot start", "i", i)
	}
	if took := time.Since(began); took > time.Second {
		t.Fatalf("200 log statements took %v while the runtime could not start", took)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the background start never began")
	}
	// The delivery goroutine retries with backoff while the one start runs.
	time.Sleep(7 * time.Second)
	if n := activations.Load(); n != 1 {
		t.Fatalf("%d starts, want one start and its failure remembered", n)
	}
	counts := h.Counts()
	if counts.Accepted != 200 || counts.Queued != 200 || !counts.Failing || failures.Load() != 1 {
		t.Fatalf("counts %+v failures %d, want 200 queued and one failure transition", counts, failures.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	began = time.Now()
	counts, err := h.Close(ctx)
	var refused *ResolutionError
	if !errors.As(err, &refused) || refused.Status != resolution.RuntimeUnavailable || !errors.Is(err, bootstrap.ErrActivationRefused) {
		t.Fatalf("Close: %v", err)
	}
	if counts.Failed != 200 || counts.Abandoned != 0 || counts.Written != 0 || time.Since(began) > 2*time.Second {
		t.Fatalf("Close counts %+v after %v", counts, time.Since(began))
	}
	if n := activations.Load(); n != 1 {
		t.Fatalf("Close started the runtime again within the retry wait: %d starts", n)
	}
}
