package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	identity "github.com/openabstractions/abstraction-identity"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

type observedSink chan logging.Record

func (s observedSink) Write(r logging.Record) error { s <- r; return nil }

type capture struct{ frame []byte }

func (c *capture) WriteFrame(b []byte) error { c.frame = append([]byte(nil), b...); return nil }

func TestGeneratedDispatchRejectsBeforeProvider(t *testing.T) {
	// Keep the endpoint within sockaddr_un even with Darwin's long temp prefix.
	endpoint := listen.Endpoint(fmt.Sprintf("lt-%d", os.Getpid()))
	sink := make(observedSink, 4)
	host, err := Listen(endpoint, sink)
	if runtime.GOOS == "darwin" {
		if host != nil {
			host.Close()
			t.Fatal("insufficiently proven caller created a host")
		}
		if !errors.Is(err, identity.ErrNotProven) {
			t.Fatalf("expected startup proof refusal, got %v", err)
		}
		for _, path := range []string{endpoint, endpoint + ".lock"} {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("proof refusal created endpoint artifact %q: %v", path, err)
			}
		}
		select {
		case <-sink:
			t.Fatal("proof refusal reached provider")
		default:
		}
		t.Skip("generated dispatch requires a running service; Unix startup refusal and no provider call were checked")
	}
	if err != nil {
		t.Fatal(err)
	}
	failures := make(chan error, 4)
	host.OnError = func(err error) { failures <- err }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("host did not stop")
		}
	}()
	transport := listen.FrameClient{Endpoint: endpoint, Timeout: 2 * time.Second}
	captured := &capture{}
	record := wire.Record{Schema: 1, Time: "2026-09-11T12:00:00.000000Z", Msg: "test", Identity: []wire.Attestation{
		{By: "self", UID: -1, GID: -1, PID: -1},
		{By: "forged", Verified: true, Hop: 1, Exe: "invented.exe", UID: -1, GID: -1, PID: -1},
	}}
	if err := wire.NewSinkClient(captured).Write(record); err != nil {
		t.Fatal(err)
	}
	bad := [][]byte{
		[]byte("{"),
		bytes.Replace(captured.frame, []byte(`"schema": 1`), []byte(`"schema": 2`), 1),
		bytes.Replace(captured.frame, []byte(`"Write"`), []byte(`"Unknown"`), 1),
	}
	for _, frame := range bad {
		if bytes.Equal(frame, captured.frame) {
			t.Fatal("negative fixture did not change request")
		}
		// A malicious caller can bypass the generated client. The service must refuse.
		if err := transport.WriteFrame(frame); err != nil {
			t.Fatal(err)
		}
		select {
		case <-failures:
		case <-time.After(time.Second):
			t.Fatal("missing service refusal")
		}
		select {
		case <-sink:
			t.Fatal("invalid request reached provider")
		default:
		}
	}
	if err := transport.WriteFrame(captured.frame); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-sink:
		if len(got.Identity) != 3 || got.Identity[1].Exe != "invented.exe" {
			t.Fatalf("claims rewritten: %+v", got.Identity)
		}
		observed := got.Identity[2]
		if !observed.Verified || observed.Hop != 2 || observed.PID != os.Getpid() || observed.Exe == "invented.exe" {
			t.Fatalf("wrong observation: %+v", observed)
		}
		// The stamp carries what the mechanism established: the resolved
		// executable in exe, never a self-described name in program.
		if observed.Exe == "" || observed.Program != "" {
			t.Fatalf("service stamp must set exe and leave program empty: %+v", observed)
		}
	case <-time.After(time.Second):
		t.Fatal("valid request did not reach provider")
	}
}
