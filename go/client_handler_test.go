package logging_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"runtime"
	"strings"
	"testing"
	"time"

	facade "github.com/openabstractions/abstraction-facade/go-core/go/abstraction/facade"
	"github.com/openabstractions/abstraction-facade/go-core/resolution"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	"github.com/openabstractions/abstraction-logging/go/client"
	"github.com/openabstractions/abstraction-logging/go/service"
)

type recordChan chan logging.Record

func (c recordChan) Write(r logging.Record) error { c <- r; return nil }

func skipWithoutProgramProof(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("Program-bound local calls remain unproven on Darwin")
	}
}

func startLogService(t *testing.T, name string) (string, recordChan) {
	t.Helper()
	endpoint := listen.Endpoint(fmt.Sprintf("%s-%d", name, os.Getpid()))
	records := make(recordChan, 8)
	host, err := service.Listen(endpoint, records)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		host.Close()
		<-done
	})
	return endpoint, records
}

func startResolver(t *testing.T, name, logEndpoint string, ready bool) string {
	t.Helper()
	endpoint := listen.Endpoint(fmt.Sprintf("%s-%d", name, os.Getpid()))
	catalog, err := resolution.New([]resolution.Candidate{{Ready: ready, Reference: facade.ServiceReference{
		Provider: "logging-test", Capability: "abstraction.logging", Contract: "abstraction.logging/sink@1",
		Scope: facade.ScopeLocal, Transport: resolution.LocalTransport, Endpoint: logEndpoint, Guarantees: []string{},
	}}})
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
	t.Cleanup(func() {
		cancel()
		host.Close()
		<-done
	})
	return endpoint
}

func thisProgram(t *testing.T) listen.ServerExpectation {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		current, err := user.Current()
		if err != nil {
			t.Fatal(err)
		}
		return listen.ServerExpectation{Principal: identity.User{Kind: "windows", SID: current.Uid}, Program: exe}
	}
	return listen.ServerExpectation{Principal: identity.User{Kind: "posix", UID: os.Geteuid(), GID: -1}, Program: exe}
}

func receive(t *testing.T, records recordChan) logging.Record {
	t.Helper()
	select {
	case r := <-records:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no record reached the service")
		return logging.Record{}
	}
}

func assertThroughService(t *testing.T, r logging.Record, program, msg string) {
	t.Helper()
	if r.Msg != msg || r.Attrs["k"] != "v" || len(r.Identity) != 2 {
		t.Fatalf("record %+v", r)
	}
	if w := r.Identity[0]; w.By != logging.BySelf || w.Verified || w.Hop != 0 || w.Program != program {
		t.Fatalf("writer claim %+v", w)
	}
	if s := r.Identity[1]; !s.Verified || s.Hop != 1 || !strings.HasPrefix(s.By, "identity/") || s.Exe == "" {
		t.Fatalf("service stamp %+v", s)
	}
}

func TestNewClientHandlerWritesThroughAResolvedClient(t *testing.T) {
	skipWithoutProgramProof(t)
	endpoint, records := startLogService(t, "lch")
	handler := logging.NewClientHandler(client.New(endpoint), &logging.Options{Program: "adopter"})
	slog.New(handler).Info("client handler", "k", "v")
	assertThroughService(t, receive(t, records), "adopter", "client handler")
	if err := handler.Check(context.Background()); err != nil {
		t.Fatalf("a client handler has nothing to resolve: %v", err)
	}
}

func TestResolveVerifiedResolvesEagerly(t *testing.T) {
	skipWithoutProgramProof(t)
	logEndpoint, records := startLogService(t, "lrv")
	resolver := startResolver(t, "lrr", logEndpoint, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler, err := logging.ResolveVerified(ctx, "eager", resolver, thisProgram(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Check(ctx); err != nil {
		t.Fatalf("check after resolve: %v", err)
	}
	slog.New(handler).Warn("resolved eagerly", "k", "v")
	r := receive(t, records)
	assertThroughService(t, r, "eager", "resolved eagerly")
	if r.Level != logging.LevelWarn {
		t.Fatalf("level %v", r.Level)
	}
}

func TestResolveVerifiedReportsTypedRefusal(t *testing.T) {
	skipWithoutProgramProof(t)
	logEndpoint, _ := startLogService(t, "lnr")
	resolver := startResolver(t, "lnrr", logEndpoint, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler, err := logging.ResolveVerified(ctx, "refused", resolver, thisProgram(t))
	var refusal *logging.ResolutionError
	if handler != nil || !errors.As(err, &refusal) || refusal.Status != facade.ResolutionStatusNotReady {
		t.Fatalf("handler %v, error %v", handler, err)
	}
	lazy := logging.NewResolvedHandler("refused", resolver, thisProgram(t)).(*logging.Handler)
	if err := lazy.Check(ctx); !errors.As(err, &refusal) || refusal.Status != facade.ResolutionStatusNotReady {
		t.Fatalf("check on a lazy handler: %v", err)
	}
}

func TestResolveVerifiedRefusesAnotherServer(t *testing.T) {
	skipWithoutProgramProof(t)
	logEndpoint, _ := startLogService(t, "lws")
	resolver := startResolver(t, "lwsr", logEndpoint, true)
	wrong := thisProgram(t)
	wrong.Program = wrong.Program + ".not-this-program"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if handler, err := logging.ResolveVerified(ctx, "wrong", resolver, wrong); handler != nil || err == nil {
		t.Fatalf("resolved against an untrusted server: %v %v", handler, err)
	}
}
