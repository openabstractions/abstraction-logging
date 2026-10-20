package logging_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	facade "github.com/openabstractions/abstraction-facade/go-core/go/abstraction/facade"
	"github.com/openabstractions/abstraction-facade/go-core/resolution"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
)

// lockedBuffer is a native handler's output, written from the delivery
// goroutine and read by the test.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// With no OA runtime installed, DefaultWithNative's records go to the native
// handler the adopter installed, after one line naming the reason [LOG-S15].
// This runs the real installed-runtime selection, which finds none in a test
// binary.
func TestDefaultWithNativeWritesLocallyWithoutOA(t *testing.T) {
	var out lockedBuffer
	h := logging.DefaultWithNative("my-app", slog.NewTextHandler(&out, nil))
	logger := slog.New(h)
	logger.Info("worker started", "component", "indexer")
	logger.Warn("disk nearly full", "drive", "D:")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	counts, err := h.Close(ctx)
	if err != nil {
		t.Fatalf("Close with a native handler and no OA: %v", err)
	}
	if counts.Native != 2 || counts.Written != 0 || counts.Failed != 0 || counts.Abandoned != 0 {
		t.Fatalf("counts %+v, want two records counted Native", counts)
	}
	text := out.String()
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) != 3 || !strings.Contains(lines[0], "level=WARN") ||
		!strings.Contains(lines[0], `msg="abstraction.logging/sink@1 not offered: runtime_unavailable"`) ||
		!strings.Contains(lines[1], `msg="worker started" component=indexer`) ||
		!strings.Contains(lines[2], `level=WARN msg="disk nearly full" drive=D:`) {
		t.Fatalf("native output:\n%s", text)
	}
}

// startCatalogResolver serves a resolver over candidates at endpoint. With
// no candidates its answer is unavailable.
func startCatalogResolver(t *testing.T, endpoint string, candidates []resolution.Candidate) func() {
	t.Helper()
	catalog, err := resolution.New(candidates)
	if err != nil {
		t.Fatal(err)
	}
	host, err := resolution.Listen(endpoint, catalog, func(*identity.Peer, facade.ServiceReference) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			host.Close()
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// A runtime whose resolver answers unavailable sends records to native; once
// the runtime offers logging, the next recheck sends them to the service, and
// native gets one line for each transition [LOG-S15].
func TestNativeFallbackHandsOverWhenTheServiceIsOffered(t *testing.T) {
	skipWithoutProgramProof(t)
	saved := logging.NativeRecheckInterval
	logging.NativeRecheckInterval = 200 * time.Millisecond
	t.Cleanup(func() { logging.NativeRecheckInterval = saved })

	logEndpoint, records := startLogService(t, "lnf")
	resolver := listen.Endpoint(fmt.Sprintf("lnfr-%d", os.Getpid()))
	stop := startCatalogResolver(t, resolver, nil)
	var out lockedBuffer
	h := logging.NewResolvedNativeHandler("handover", resolver, thisProgram(t), slog.NewTextHandler(&out, nil))
	logger := slog.New(h)
	logger.Info("before the service")
	waitFor(t, "the native record", func() bool { return h.Counts().Native == 1 })
	if text := out.String(); !strings.Contains(text, "not offered: unavailable") || !strings.Contains(text, "before the service") {
		t.Fatalf("native output:\n%s", text)
	}

	stop()
	startCatalogResolver(t, resolver, []resolution.Candidate{{Ready: true, Reference: facade.ServiceReference{
		Provider: "logging-test", Capability: "abstraction.logging", Contract: "abstraction.logging/sink@1",
		Scope: facade.ScopeLocal, Transport: resolution.LocalTransport, Endpoint: logEndpoint, Guarantees: []string{},
	}}})
	time.Sleep(2 * logging.NativeRecheckInterval)
	logger.Info("after the service")
	if r := receive(t, records); r.Msg != "after the service" {
		t.Fatalf("service got %q", r.Msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	counts, err := h.Close(ctx)
	if err != nil || counts.Native != 1 || counts.Written != 1 {
		t.Fatalf("Close: %+v, %v", counts, err)
	}
	text := out.String()
	if !strings.Contains(text, "offered again; records go to the service") || strings.Contains(text, "after the service") {
		t.Fatalf("native output:\n%s", text)
	}
}
