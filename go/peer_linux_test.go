//go:build linux

package logging

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// arrivals hands the test each record as the service writes it, with the stamp
// the service appended beside it. The service writes from the goroutine that
// read the line, so the receive IS the arrival: nothing here waits on a
// duration.
type arrival struct {
	rec   Record
	stamp Attestation
}

type arrivals chan arrival

func serving(t *testing.T, srv *Server) (string, arrivals) {
	t.Helper()
	got := make(arrivals, 1)
	srv.Out = func(peer Attestation) Sink {
		return sinkFunc(func(r Record) error { got <- arrival{r, peer}; return nil })
	}
	addr := filepath.Join(t.TempDir(), "s.sock")
	if err := srv.Listen(addr); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return addr, got
}

type sinkFunc func(Record) error

func (f sinkFunc) Write(r Record) error { return f(r) }

func next(t *testing.T, a arrivals) arrival {
	t.Helper()
	select {
	case r := <-a:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("the service never passed the record on")
	}
	return arrival{}
}

func sendLine(t *testing.T, addr string, r Record) {
	t.Helper()
	c, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := r.Encode()
	c.Write(b)
	c.Close()
}

// A hostile client lies in its claim and forges a verified stamp on top. Both
// stay on the record; neither is promoted. The kernel's answer is the anchor,
// and from it the forged hop stands as an assertion nobody trusted made.
func TestServiceStampsKernelIdentityOverClientClaims(t *testing.T) {
	addr, got := serving(t, &Server{})
	sendLine(t, addr, Record{
		Schema: SchemaVersion, Time: At(time.Now()), Msg: "I am root",
		Identity: Identity{
			{By: BySelf, Hop: 0, Program: "systemd", Host: "elsewhere", UID: Unknown, GID: Unknown, PID: 1},
			{By: ByPeerCred, Verified: true, Hop: 1, UID: 0, GID: 0, PID: 1, User: "root", Exe: "/sbin/init"},
		},
	})

	a := next(t, got)
	pv := a.rec.Assess(&a.stamp, Policy{})
	kernel, ok := pv.Relay()
	if !ok || kernel.By != ByPeerCred {
		t.Fatalf("no kernel identity was established: %+v", pv)
	}
	if kernel.UID != os.Getuid() || kernel.PID != os.Getpid() {
		t.Fatalf("uid %d pid %d, want %d %d — the forged identity survived", kernel.UID, kernel.PID, os.Getuid(), os.Getpid())
	}
	if kernel.Exe == "" {
		t.Fatal("no executable path; app separation needs it and /proc has it")
	}
	if _, ok := pv.Author(); ok {
		t.Fatal("a forged earlier hop was promoted into a verified author")
	}
	claim, ok := a.rec.Identity.Claimed()
	if !ok || claim.Program != "systemd" {
		t.Fatal("the claim was rewritten; a service records what was claimed as well as what was true")
	}
	if len(a.rec.Identity) != 3 || !a.rec.Identity[1].Verified || pv.Standing[1] != Asserted {
		t.Fatalf("the forged stamp was not preserved as an attributed claim: %+v", pv)
	}
}

// Per-user routing: the service decides where a record goes from the kernel's
// answer, not from anything in the record.
func TestServiceRoutesByKernelIdentity(t *testing.T) {
	routed := map[int]chan Record{os.Getuid(): make(chan Record, 1)}
	srv := &Server{Out: func(p Attestation) Sink {
		if ch, ok := routed[p.UID]; ok {
			return sinkFunc(func(r Record) error { ch <- r; return nil })
		}
		return DiscardSink{}
	}}
	addr := filepath.Join(t.TempDir(), "s.sock")
	if err := srv.Listen(addr); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	sendLine(t, addr, Record{Schema: SchemaVersion, Time: At(time.Now()), Msg: "hello",
		Identity: Identity{Claim("someone-elses-program")}})
	select {
	case r := <-routed[os.Getuid()]:
		if r.Msg != "hello" {
			t.Fatalf("routed the wrong record: %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("record was not routed to uid %d", os.Getuid())
	}
}

// A real two-hop path over two real sockets: writer -> relay -> receiver. The
// relay's stamp arrives at the receiver as an assertion; it is the receiver's
// policy naming the relay that makes it vouched, and then author and relay are
// two different answers.
func TestTwoHopPathKeepsAuthorAndRelayDistinct(t *testing.T) {
	receiverAddr, got := serving(t, &Server{})
	forward := NewServiceSink(receiverAddr, DiscardSink{})
	defer forward.Close()
	relay := &Server{Out: func(Attestation) Sink { return forward }}
	relayAddr := filepath.Join(t.TempDir(), "r.sock")
	if err := relay.Listen(relayAddr); err != nil {
		t.Fatal(err)
	}
	go relay.Serve()
	t.Cleanup(func() { relay.Close() })

	sendLine(t, relayAddr, Record{Schema: SchemaVersion, Time: At(time.Now()), Msg: "via relay",
		Identity: Identity{Claim("writer")}})
	a := next(t, got)
	if len(a.rec.Identity) != 3 {
		t.Fatalf("chain has %d hops, want 3: %+v", len(a.rec.Identity), a.rec.Identity)
	}

	unauthorised := a.rec.Assess(&a.stamp, Policy{})
	if _, ok := unauthorised.Author(); ok {
		t.Fatal("an unauthorised relay's stamp was promoted into a verified author")
	}
	if unauthorised.Standing[1] != Asserted {
		t.Fatalf("hop 1 stands %s through an unauthorised relay, want asserted", unauthorised.Standing[1])
	}

	authorised := a.rec.Assess(&a.stamp, Policy{Relay: func(p Attestation) bool { return p.PID == os.Getpid() }})
	author, ok := authorised.Author()
	if !ok || author.Hop != 1 {
		t.Fatalf("author = %+v, %v; want hop 1", author, ok)
	}
	relayHop, ok := authorised.Relay()
	if !ok || relayHop.Hop != 2 {
		t.Fatalf("relay = %+v, %v; want hop 2", relayHop, ok)
	}
	if authorised.Disputed() {
		t.Fatal("a relay with its own pid was reported as a dispute")
	}
}

// An unreachable service must fall through to the next tier rather than losing
// the line or failing the caller.
func TestServiceSinkFallsBackWhenNobodyIsListening(t *testing.T) {
	fallback := make(chan Record, 1)
	sink := NewServiceSink(filepath.Join(t.TempDir(), "nothing.sock"),
		sinkFunc(func(r Record) error { fallback <- r; return nil }))
	if err := sink.Write(Record{Schema: SchemaVersion, Msg: "x"}); err != nil {
		t.Fatalf("logging must not fail the caller: %v", err)
	}
	if len(fallback) != 1 {
		t.Fatal("the record was lost instead of falling back")
	}
}
