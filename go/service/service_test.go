package service

import (
	"bytes"
	"context"
	"fmt"
	"os"
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
	endpoint := listen.Endpoint(fmt.Sprintf("log-test-%d-%d", os.Getpid(), time.Now().UnixNano()))
	sink := make(observedSink, 4)
	host, err := Listen(endpoint, sink)
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
		{By: "self", Uid: -1, Gid: -1, Pid: -1},
		{By: "forged", Verified: true, Hop: 1, Exe: "invented.exe", Uid: -1, Gid: -1, Pid: -1},
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
	case <-time.After(time.Second):
		t.Fatal("valid request did not reach provider")
	}
}
