package logging

import (
	"errors"
	"testing"
)

var stampW = Attestation{By: ByPeerCred, Verified: true, Host: "h", UID: 1000, GID: 1000, PID: 100, Exe: "/usr/bin/w"}

func TestARecordWithoutAWriterClaimGetsAnUnclaimedHopZero(t *testing.T) {
	input := rec()
	input.Attrs = map[string]string{"k": "v"}
	got := (&Server{}).Accept(input, stampW, nil)

	if len(got.Identity) != 2 {
		t.Fatalf("chain %+v", got.Identity)
	}
	if got.Identity[0] != Unclaimed() {
		t.Fatalf("hop 0 %+v", got.Identity[0])
	}
	first := got.Identity[0]
	if first.By != ByUnclaimed || first.Verified || first.Hop != 0 || first.UID != Unknown || first.GID != Unknown || first.PID != Unknown || first.Program != "" || first.Exe != "" {
		t.Fatalf("hop 0 asserts something: %+v", first)
	}
	if s := got.Identity[1]; s.Hop != 1 || !s.Verified || s.Exe != stampW.Exe {
		t.Fatalf("stamp %+v", s)
	}
	if got.Attrs[AttrWriterClaim] != "absent" || got.Attrs["k"] != "v" {
		t.Fatalf("attrs %+v", got.Attrs)
	}
	if len(input.Identity) != 0 || len(input.Attrs) != 1 {
		t.Fatalf("caller record rewritten: %+v", input)
	}

	stamp := got.Identity[1]
	pv := got.Assess(&stamp, Policy{})
	if s := standings(pv); s != "claimed established" {
		t.Fatalf("standing %q", s)
	}
	if a, ok := pv.Author(); !ok || a.Hop != 1 || a.Exe != stampW.Exe {
		t.Fatalf("author %+v %v", a, ok)
	}
	if r, ok := pv.Relay(); !ok || r.Hop != 1 {
		t.Fatalf("relay %+v %v", r, ok)
	}
	if _, ok := pv.Chain.Claimed(); ok {
		t.Fatal("an unclaimed hop 0 was read as the writer's claim")
	}
	if pv.Disputed() {
		t.Fatal("no claim cannot be a dispute")
	}
}

func TestAWriterClaimIsKeptAsHopZero(t *testing.T) {
	got := (&Server{}).Accept(rec(writer), stampW, nil)
	if len(got.Identity) != 2 || got.Identity[0] != writer || got.Identity[1].Hop != 1 {
		t.Fatalf("chain %+v", got.Identity)
	}
	if _, present := got.Attrs[AttrWriterClaim]; present {
		t.Fatalf("claimed record marked unclaimed: %+v", got.Attrs)
	}
}

func TestAnUnattestablePeerStillKeepsHopZeroForTheWriter(t *testing.T) {
	got := (&Server{}).Accept(rec(), Attestation{}, errors.New("no peer credentials"))
	if len(got.Identity) != 1 || got.Identity[0] != Unclaimed() {
		t.Fatalf("chain %+v", got.Identity)
	}
	if got.Attrs[AttrWriterClaim] != "absent" || got.Attrs["logging.peer_error"] != "no peer credentials" {
		t.Fatalf("attrs %+v", got.Attrs)
	}
	claimed := (&Server{}).Accept(rec(writer), Attestation{}, errors.New("no peer credentials"))
	if len(claimed.Identity) != 1 || claimed.Identity[0] != writer {
		t.Fatalf("claimed chain %+v", claimed.Identity)
	}
}

func TestUnclaimedSurvivesEncoding(t *testing.T) {
	got := (&Server{}).Accept(rec(), stampW, nil)
	line, err := got.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeRecord(line)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Identity) != 2 || back.Identity[0] != Unclaimed() || back.Attrs[AttrWriterClaim] != "absent" {
		t.Fatalf("decoded %+v", back)
	}
}
