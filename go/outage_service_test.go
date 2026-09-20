package logging_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	facade "github.com/openabstractions/abstraction-facade/go-core/go/abstraction/facade"
	"github.com/openabstractions/abstraction-facade/go-core/resolution"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	"github.com/openabstractions/abstraction-logging/go/service"
)

// fakeRuntime is a resolver and a logging service at fixed endpoints that a
// test stops and starts again, as a supervisor restarts the runtime.
type fakeRuntime struct {
	t                    *testing.T
	resolver, logService string
	records              recordChan
	stops                []func()
}

func newFakeRuntime(t *testing.T, name string) *fakeRuntime {
	r := &fakeRuntime{t: t, records: make(recordChan, 256),
		resolver:   listen.Endpoint(fmt.Sprintf("%s-r-%d", name, os.Getpid())),
		logService: listen.Endpoint(fmt.Sprintf("%s-l-%d", name, os.Getpid()))}
	t.Cleanup(r.stop)
	return r
}

func (r *fakeRuntime) start() {
	r.t.Helper()
	logHost, err := service.Listen(r.logService, r.records)
	if err != nil {
		r.t.Fatal(err)
	}
	catalog, err := resolution.New([]resolution.Candidate{{Ready: true, Reference: facade.ServiceReference{
		Provider: "logging-outage-test", Capability: "abstraction.logging", Contract: "abstraction.logging/sink@1",
		Scope: facade.ScopeLocal, Transport: resolution.LocalTransport, Endpoint: r.logService, Guarantees: []string{},
	}}})
	if err != nil {
		r.t.Fatal(err)
	}
	resolverHost, err := resolution.Listen(r.resolver, catalog, func(*identity.Peer, facade.ServiceReference) bool { return true })
	if err != nil {
		r.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = logHost.Serve(ctx) }()
	go func() { defer wg.Done(); _ = resolverHost.Serve(ctx) }()
	r.stops = append(r.stops, func() {
		cancel()
		logHost.Close()
		resolverHost.Close()
		wg.Wait()
	})
}

func (r *fakeRuntime) stop() {
	for _, stop := range r.stops {
		stop()
	}
	r.stops = nil
}

// The runtime disappears under a bound handler and a supervisor starts it
// again at the same endpoints. Records logged during the outage are delivered
// in order after the restart, behind one gap record, and none is Failed
// [LOG-S12, LOG-S13].
func TestBoundHandlerDeliversRecordsLoggedWhileTheRuntimeRestarts(t *testing.T) {
	skipWithoutProgramProof(t)
	runtime := newFakeRuntime(t, "lout")
	runtime.start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	handler, err := logging.ResolveVerified(ctx, "outage", runtime.resolver, thisProgram(t))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(handler)
	logger.Info("before", "k", "v")
	if r := receive(t, runtime.records); r.Msg != "before" {
		t.Fatalf("first record %q", r.Msg)
	}

	runtime.stop()
	for i := range 5 {
		logger.Info(fmt.Sprint("during ", i), "k", "v")
	}
	waitFor(t, "the failure to be seen", func() bool { return handler.Counts().Failing })
	time.Sleep(300 * time.Millisecond) // several retries against the stopped runtime
	if c := handler.Counts(); c.Written != 1 || c.Queued != 5 || c.Failed != 0 {
		t.Fatalf("counts during the outage %+v", c)
	}
	runtime.start()

	gap := receive(t, runtime.records)
	if gap.Msg != logging.GapMessage || gap.Level != logging.LevelWarn || gap.Attrs["dropped"] != "0" ||
		gap.Attrs["since"] == "" || gap.Attrs["until"] == "" || gap.Identity[0].Program != "outage" {
		t.Fatalf("gap record %+v", gap)
	}
	for i := range 5 {
		if r := receive(t, runtime.records); r.Msg != fmt.Sprint("during ", i) {
			t.Fatalf("record %d after the restart is %q", i, r.Msg)
		}
	}
	c, err := handler.Close(ctx)
	if err != nil || c.Written != 6 || c.Failed != 0 || c.Dropped != 0 || c.Failing {
		t.Fatalf("close %+v, %v", c, err)
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
