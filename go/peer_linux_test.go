//go:build linux

package logging

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// arrivals hands the test each record as the service writes it. The service
// writes from the goroutine that read the line, so the receive IS the arrival:
// nothing here waits on a duration.
type arrivals chan Record

func (a arrivals) Write(r Record) error { a <- r; return nil }

func next(t *testing.T, a arrivals) Record {
	t.Helper()
	select {
	case r := <-a:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("the service never passed the record on")
	}
	return Record{}
}

func serving(t *testing.T, out func(Attestation) Sink) string {
	t.Helper()
	addr := filepath.Join(t.TempDir(), "s.sock")
	srv := &Server{Out: out}
	if err := srv.Listen(addr); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return addr
}

// The claim a client makes about itself must not survive contact with the
// kernel's answer.
//
// This is the whole reason the service tier exists. A client that forges a
// verified attestation must end up attributed to the uid the kernel reports for
// its connection — otherwise "user separation" is a labelling convention that
// any local process can defeat by typing a different string.
func TestServiceStampsKernelIdentityOverClientClaims(t *testing.T) {
	got := make(arrivals, 1)
	addr := serving(t, func(Attestation) Sink { return got })

	c, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	// A hostile client: lies in its own claim, and forges a verified one on top.
	forged := Record{
		Schema: SchemaVersion,
		Time:   At(time.Now()),
		Msg:    "I am root",
		Identity: Identity{
			{By: BySelf, Hop: 0, Program: "systemd", Host: "elsewhere", UID: Unknown, GID: Unknown, PID: 1},
			{By: ByPeerCred, Verified: true, Hop: 1, UID: 0, GID: 0, PID: 1, User: "root", Exe: "/sbin/init"},
		},
	}
	b, _ := forged.Encode()
	c.Write(b)
	c.Close()

	r := next(t, got)
	kernel, ok := r.Identity.Verified()
	if !ok {
		t.Fatal("no kernel identity was stamped")
	}
	if kernel.By != ByPeerCred {
		t.Fatalf("verified by %q, want %q", kernel.By, ByPeerCred)
	}
	if kernel.UID != os.Getuid() {
		t.Fatalf("UID = %d, want %d — the forged identity survived", kernel.UID, os.Getuid())
	}
	if kernel.PID != os.Getpid() {
		t.Fatalf("PID = %d, want %d", kernel.PID, os.Getpid())
	}
	if kernel.Exe == "" {
		t.Fatal("no executable path; app separation needs it and /proc has it")
	}
	// The lie is preserved, in the field that is documented as a claim. Deleting
	// it would destroy evidence; believing it would be the bug.
	claim, ok := r.Identity.Claimed()
	if !ok || claim.Program != "systemd" {
		t.Fatal("the claim was rewritten; a service must record what was claimed as well as what was true")
	}
	for _, a := range r.Identity {
		if a.Verified && a.PID == 1 {
			t.Fatal("a client's forged attestation arrived still marked verified")
		}
	}
}

// Per-user routing: the service decides where a record goes from the kernel's
// answer, not from anything in the record.
func TestServiceRoutesByKernelIdentity(t *testing.T) {
	routed := map[int]arrivals{}
	mine := make(arrivals, 1)
	routed[os.Getuid()] = mine
	addr := serving(t, func(p Attestation) Sink {
		if s, ok := routed[p.UID]; ok {
			return s
		}
		return DiscardSink{}
	})

	c, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	r := Record{Schema: SchemaVersion, Time: At(time.Now()), Msg: "hello",
		Identity: Identity{Claim("someone-elses-program")}}
	b, _ := r.Encode()
	c.Write(b)
	c.Close()

	if next(t, mine).Msg != "hello" {
		t.Fatalf("record was not routed to uid %d", os.Getuid())
	}
}

// The client sink must never be able to send a verified attestation at all.
func TestServiceSinkStripsAnyClientClaimOfVerification(t *testing.T) {
	got := make(arrivals, 1)
	addr := serving(t, func(Attestation) Sink { return got })

	sink := NewServiceSink(addr, DiscardSink{})
	defer sink.Close()
	sink.Write(Record{Schema: SchemaVersion, Time: At(time.Now()), Msg: "x",
		Identity: Identity{{By: ByPeerCred, Verified: true, UID: 0, User: "root"}}})

	kernel, ok := next(t, got).Identity.Verified()
	if !ok {
		t.Fatal("no kernel identity was stamped")
	}
	if kernel.UID != os.Getuid() {
		t.Fatalf("UID = %d, want %d", kernel.UID, os.Getuid())
	}
}

// An unreachable service must fall through to the next tier rather than losing
// the line or failing the caller.
func TestServiceSinkFallsBackWhenNobodyIsListening(t *testing.T) {
	fallback := make(arrivals, 1)
	sink := NewServiceSink(filepath.Join(t.TempDir(), "nothing.sock"), fallback)
	if err := sink.Write(Record{Schema: SchemaVersion, Msg: "x"}); err != nil {
		t.Fatalf("logging must not fail the caller: %v", err)
	}
	if len(fallback) != 1 {
		t.Fatal("the record was lost instead of falling back")
	}
}
