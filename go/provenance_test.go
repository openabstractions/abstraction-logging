package logging

import (
	"strings"
	"testing"
	"time"
)

var (
	writer    = Attestation{By: BySelf, Hop: 0, Program: "w", Host: "h", UID: Unknown, GID: Unknown, PID: 100}
	relayW    = Attestation{By: ByPeerCred, Verified: true, Hop: 1, Host: "h", UID: 1000, GID: 1000, PID: 100, Exe: "/usr/bin/w"}
	receiveR  = Attestation{By: ByPeerCred, Verified: true, Hop: 2, Host: "h", UID: 0, GID: 0, PID: 7, Exe: "/usr/bin/relay"}
	authRelay = Policy{Relay: func(p Attestation) bool { return p.Exe == "/usr/bin/relay" }}
	key       = []byte("operator-installed")
)

func rec(chain ...Attestation) Record {
	return Record{Schema: SchemaVersion, Time: At(time.Unix(0, 0).UTC()), Msg: "m", Identity: Identity(chain)}
}

func standings(pv Provenance) string {
	var s []string
	for _, st := range pv.Standing {
		s = append(s, st.String())
	}
	return strings.Join(s, " ")
}

func TestTwoHopPathIsWalkedFromTheAnchor(t *testing.T) {
	r := rec(writer, relayW, receiveR)
	pv := r.Assess(&receiveR, authRelay)
	if got := standings(pv); got != "claimed vouched established" {
		t.Fatalf("standing %q", got)
	}
	if a, ok := pv.Author(); !ok || a.Hop != 1 {
		t.Fatalf("author %+v %v", a, ok)
	}
	if rl, ok := pv.Relay(); !ok || rl.Hop != 2 {
		t.Fatalf("relay %+v %v", rl, ok)
	}
	if pv.Disputed() {
		t.Fatal("relay pid 7 against writer pid 100 reported as a dispute")
	}
}

func TestOnlyTheImmediatePeerEstablishedLeavesTheAuthorUnverified(t *testing.T) {
	r := rec(writer, relayW, receiveR)
	pv := r.Assess(&receiveR, Policy{})
	if got := standings(pv); got != "claimed asserted established" {
		t.Fatalf("standing %q", got)
	}
	if _, ok := pv.Author(); ok {
		t.Fatal("an unauthorised relay's stamp was promoted to author")
	}
	if _, ok := pv.Relay(); !ok {
		t.Fatal("the established immediate peer was not reported as the relay")
	}
}

func TestForgedEarlierHopStandsAsserted(t *testing.T) {
	forged := relayW
	forged.UID, forged.Exe = 0, "/sbin/init"
	stamp := receiveR
	stamp.Exe = "/usr/bin/curl"
	r := rec(writer, forged, stamp)
	pv := r.Assess(&stamp, authRelay)
	if got := standings(pv); got != "claimed asserted established" {
		t.Fatalf("standing %q", got)
	}
	if _, ok := pv.Author(); ok {
		t.Fatal("a forged hop was promoted to author")
	}
}

func TestNoAnchorMeansNothingIsEstablished(t *testing.T) {
	r := rec(writer, relayW, receiveR)
	if got := standings(r.Assess(nil, authRelay)); got != "claimed asserted asserted" {
		t.Fatalf("standing %q", got)
	}
	stranger := receiveR
	stranger.PID = 8
	if got := standings(r.Assess(&stranger, authRelay)); got != "claimed asserted asserted" {
		t.Fatalf("an anchor that is not the last hop still anchored: %q", got)
	}
}

func TestBoundEvidenceVouchesWithoutAPath(t *testing.T) {
	r := rec(writer, relayW)
	if err := r.Bind(1, "k1", key); err != nil {
		t.Fatal(err)
	}
	r.Identity = append(r.Identity, receiveR)
	held := Policy{Keys: map[string][]byte{"k1": key}}
	if got := standings(r.Assess(&receiveR, held)); got != "claimed vouched established" {
		t.Fatalf("standing %q", got)
	}
	if got := standings(r.Assess(&receiveR, Policy{Keys: map[string][]byte{"k1": []byte("other")}})); got != "claimed asserted established" {
		t.Fatalf("a binding under a key the assessor does not hold vouched: %q", got)
	}
}

func TestAlteredRecordBreaksTheBinding(t *testing.T) {
	r := rec(writer, relayW)
	r.Attrs = map[string]string{"bytes": "1"}
	if err := r.Bind(1, "k1", key); err != nil {
		t.Fatal(err)
	}
	held := Policy{Keys: map[string][]byte{"k1": key}}
	for name, alter := range map[string]func(*Record){
		"message":   func(x *Record) { x.Msg = "m2" },
		"attribute": func(x *Record) { x.Attrs["bytes"] = "2" },
		"level":     func(x *Record) { x.Level = LevelError },
		"claim":     func(x *Record) { x.Identity[0].Program = "systemd" },
		"subject":   func(x *Record) { x.Identity[1].UID = 0 },
	} {
		x := r
		x.Attrs = map[string]string{"bytes": "1"}
		x.Identity = append(Identity(nil), r.Identity...)
		alter(&x)
		x.Identity = append(x.Identity, receiveR)
		if got := standings(x.Assess(&receiveR, held)); got != "claimed asserted established" {
			t.Errorf("%s altered after binding, standing %q", name, got)
		}
	}
}

func TestDisputedComparesTheWriterWithTheAttestationAboutTheWriter(t *testing.T) {
	lying := relayW
	lying.PID = 200
	direct := lying
	direct.Hop = 1
	if !rec(writer, direct).Assess(&direct, Policy{}).Disputed() {
		t.Fatal("kernel pid 200 against claimed pid 100 not disputed")
	}
	r := rec(writer, lying, receiveR)
	if !r.Assess(&receiveR, authRelay).Disputed() {
		t.Fatal("a vouched contradiction through an authorised relay not disputed")
	}
	if r.Assess(&receiveR, Policy{}).Disputed() {
		t.Fatal("an asserted contradiction counted as a dispute")
	}
}

func TestServiceAcceptAppendsAndBinds(t *testing.T) {
	s := &Server{KeyID: "k1", Key: key}
	forged := relayW
	forged.UID = 0
	in := rec(writer, forged)
	out := s.Accept(in, receiveR, nil)
	if len(in.Identity) != 2 || len(out.Identity) != 3 {
		t.Fatalf("accept rewrote its input or did not append: in %d out %d", len(in.Identity), len(out.Identity))
	}
	if out.Identity[1] != forged {
		t.Fatal("the forged hop was altered or removed")
	}
	stamp := out.Identity[2]
	if stamp.Hop != 2 || stamp.MAC == "" || stamp.Key != "k1" {
		t.Fatalf("stamp %+v", stamp)
	}
	pv := out.Assess(&stamp, Policy{Keys: map[string][]byte{"k1": key}})
	if got := standings(pv); got != "claimed asserted established" {
		t.Fatalf("standing %q", got)
	}
	failed := s.Accept(in, Attestation{}, errNoPeerCreds)
	if len(failed.Identity) != 2 || failed.Attrs["logging.peer_error"] == "" {
		t.Fatalf("a platform that could not attest left no trace: %+v", failed)
	}
}
