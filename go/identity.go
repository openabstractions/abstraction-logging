package logging

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
)

// Identity is an ordered chain, one attestation per hop, counting from the
// writer. Hop 0 is the writer's own claim about itself; hop k is what the k-th
// receiver established about the party that handed it the record.
//
// Every hop appends and nothing is removed, including a claim this layer
// believes to be false: a lie is evidence, and the reader has the context to
// judge it that the writer does not.
type Identity []Attestation

// Attestation is one party's statement about one party.
//
// Verified is the appending party's own assertion that it established the
// subject's identity rather than copied it. On the wire that assertion is the
// same bytes whether it is true or forged, so a receiver never reads trust out
// of it: what a hop is worth to a receiver is its Standing, computed by Assess
// from something the receiver holds outside the record.
type Attestation struct {
	// By names the mechanism that answered, never the party: "self" for the
	// writer's own claim, otherwise the system facility — "so_peercred",
	// "file_owner", "peer_cert". An unrecognised mechanism is unverified.
	By       string `json:"by"`
	Verified bool   `json:"verified"`
	Hop      int    `json:"hop"`

	Program string `json:"program,omitempty"`
	Host    string `json:"host,omitempty"`
	User    string `json:"user,omitempty"`
	Exe     string `json:"exe,omitempty"`

	// -1 is not established, so that 0 keeps its real meaning: uid 0 is the
	// superuser and a provider that could not determine a uid must not look
	// like one that determined root.
	UID int `json:"uid"`
	GID int `json:"gid"`
	PID int `json:"pid"`

	// Key and MAC bind this attestation to the record it was made about, under
	// a key the operator gave the party that made it. A reader holding the
	// same key can verify the stamp without trusting anything the record
	// passed through afterwards. See Record.Bind.
	Key string `json:"key,omitempty"`
	MAC string `json:"mac,omitempty"`
}

const Unknown = -1

const BySelf = "self"

// ByPeerCred is the kernel answering about a unix socket peer.
const ByPeerCred = "so_peercred"

// Claim is the writer's own description of itself: hop 0, never verified,
// recorded because almost nothing is lying and when something is, the claim is
// the evidence.
func Claim(program string) Attestation {
	host, _ := os.Hostname()
	return Attestation{
		By: BySelf, Verified: false, Hop: 0,
		Program: program, Host: host,
		UID: Unknown, GID: Unknown, PID: os.Getpid(),
	}
}

func (id Identity) Claimed() (Attestation, bool) {
	for _, a := range id {
		if a.By == BySelf && a.Hop == 0 {
			return a, true
		}
	}
	return Attestation{}, false
}

func (id Identity) By(mechanism string) (Attestation, bool) {
	for _, a := range id {
		if a.By == mechanism {
			return a, true
		}
	}
	return Attestation{}, false
}

// Standing is what one hop is worth to the party assessing the record, as
// distinct from what the hop says about itself.
//
// The ancestor is RFC 8601's Authentication-Results: a receiver records the
// checks it ran itself, and a result that arrived from outside its boundary is
// a claim however it is labelled. Claimed and Asserted read the wire; Vouched
// and Established are the assessor's own conclusion.
type Standing int

const (
	// Claimed: the hop asserts no verification. The writer's own claim.
	Claimed Standing = iota
	// Asserted: the hop asserts it was verified, and nothing the assessor
	// trusts stands behind that assertion. A forged stamp and a genuine stamp
	// from an unauthorised relay both land here.
	Asserted
	// Vouched: verified through an explicit mechanism the assessor holds — a
	// trusted path through an authorised relay, or a binding under a key.
	Vouched
	// Established: the assessor made this attestation itself.
	Established
)

func (s Standing) String() string {
	switch s {
	case Asserted:
		return "asserted"
	case Vouched:
		return "vouched"
	case Established:
		return "established"
	default:
		return "claimed"
	}
}

// Policy is what an assessor is prepared to trust beyond what it established
// itself. The zero Policy trusts nothing beyond the anchor.
type Policy struct {
	// Relay reports whether a party, as a trusted hop describes it, is
	// authorised to attest the hops beneath it. It is the cA basic constraint
	// of RFC 5280 §4.2.1.9: being on the path is not the same as being allowed
	// to vouch for the next link.
	Relay func(Attestation) bool

	// Keys are the bindings this assessor accepts, by key id. Holding a key is
	// the authorisation: a stamp bound under it stands Vouched with no path.
	Keys map[string][]byte
}

// Provenance is a record's chain with each hop's standing beside it.
type Provenance struct {
	Chain    Identity
	Standing []Standing
}

// Assess computes what each hop is worth to the caller.
//
// The anchor is the attestation the caller established itself, supplied here
// and never read from the record — RFC 5280 §6.1.1's trust anchor, delivered
// out of band. A receiving service passes the stamp it just appended; a reader
// of a file no service wrote passes nil and gets claims back, which is what a
// file can attest.
//
// From the anchor the path is walked downward, RFC 5280 §6 shape: a hop stands
// Vouched when the trusted hop above it describes a party the policy
// authorises to relay, or when the hop is bound under a key the policy holds.
// Everything else stands as what the wire says, and the wire cannot say more
// than Asserted.
func (r Record) Assess(anchor *Attestation, p Policy) Provenance {
	id := r.Identity
	n := len(id)
	st := make([]Standing, n)
	for k := n - 1; k >= 0; k-- {
		a := id[k]
		switch {
		case a.By == BySelf && a.Verified:
			st[k] = Asserted
		case a.By == BySelf || !a.Verified:
			st[k] = Claimed
		case k == n-1 && anchor != nil && sameStamp(a, *anchor):
			st[k] = Established
		case a.MAC != "" && p.Keys[a.Key] != nil && r.bindingHolds(k, p.Keys[a.Key]):
			st[k] = Vouched
		case k+1 < n && st[k+1] >= Vouched && p.Relay != nil && p.Relay(id[k+1]):
			st[k] = Vouched
		default:
			st[k] = Asserted
		}
	}
	return Provenance{Chain: id, Standing: st}
}

// sameStamp compares what was established, not where it sits: the hop is the
// record's position and the binding is added after the stamp, so an anchor
// handed over before either is still the same stamp.
func sameStamp(a, b Attestation) bool {
	a.Hop, a.Key, a.MAC = 0, "", ""
	b.Hop, b.Key, b.MAC = 0, "", ""
	return a == b
}

// Author is the attestation about the writer — hop 1, the first receiver's
// statement about the party that handed it the record — and only when the
// path from the anchor reaches it. If only the immediate peer is established,
// the author is unverified: the earliest hop asserting verification is not the
// author, it is a claim about the author.
func (pv Provenance) Author() (Attestation, bool) {
	if len(pv.Chain) > 1 && pv.Standing[1] >= Vouched {
		return pv.Chain[1], true
	}
	return Attestation{}, false
}

// Relay is the assessor's immediate peer: the hop it established itself.
func (pv Provenance) Relay() (Attestation, bool) {
	n := len(pv.Chain)
	if n > 0 && pv.Standing[n-1] == Established {
		return pv.Chain[n-1], true
	}
	return Attestation{}, false
}

// Disputed reports whether the writer's claim is contradicted by a trusted
// attestation about the writer — the same subject. A relay carrying a
// different pid or host from the writer is expected, not a dispute.
func (pv Provenance) Disputed() bool {
	claim, ok := pv.Chain.Claimed()
	if !ok {
		return false
	}
	about, ok := pv.Author()
	if !ok {
		return false
	}
	if about.Host != "" && claim.Host != "" && about.Host != claim.Host {
		return true
	}
	return about.PID != Unknown && claim.PID != Unknown && about.PID != claim.PID
}

// Bind ties the attestation at hop to this record under a key, so that a
// reader holding the key can verify it independently of the path the record
// takes afterwards.
//
// The bound bytes are the record with its chain cut at hop, hop's own MAC
// empty, rendered as canonical JSON. DKIM (RFC 6376) is the ancestor; the
// divergence is that the key is symmetric, HMAC-SHA256, because every platform
// furnishes it and the assessor that holds the key is the trust root anyway.
// A verifier can therefore forge what it verifies, and this is not the
// mechanism for a reader that must not be trusted with that.
func (r *Record) Bind(hop int, keyID string, key []byte) error {
	r.Identity[hop].Key = keyID
	r.Identity[hop].MAC = ""
	b, err := r.bound(hop)
	if err != nil {
		return err
	}
	r.Identity[hop].MAC = mac(b, key)
	return nil
}

func (r Record) bindingHolds(hop int, key []byte) bool {
	b, err := r.bound(hop)
	if err != nil {
		return false
	}
	return hmac.Equal([]byte(mac(b, key)), []byte(r.Identity[hop].MAC))
}

func mac(b, key []byte) string {
	h := hmac.New(sha256.New, key)
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// bound renders the record as it stood when hop was appended: keys sorted, no
// HTML escaping, no trailing newline. That is RFC 8785's shape for the ASCII
// this layer writes; the string escaping beyond ASCII is unproven against a
// second language.
func (r Record) bound(hop int) ([]byte, error) {
	if r.Schema == 0 {
		r.Schema = SchemaVersion
	}
	r.Identity = append(Identity(nil), r.Identity[:hop+1]...)
	r.Identity[hop].MAC = ""
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	e := json.NewEncoder(&out)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte("\n")), nil
}

// An Attester answers "who is on the other end of this?", and every useful
// implementation is one the operating system already operates: SO_PEERCRED on
// a unix socket, a named pipe client token, file ownership, a container's uid
// mapping. This package mints no identity. Where the OS offers nothing the
// answer is an error, never a plausible guess.
type Attester interface {
	Attest(conn any, hop int) (Attestation, error)
}

// Separation is what a sink can actually enforce. Naming the three answers
// stops "the service will handle it" from standing in for a design.
type Separation int

const (
	// SeparationNone: one file everyone writes. Attribution is by claim only.
	SeparationNone Separation = iota
	// SeparationOwner: one file per user, enforced by the filesystem. Cannot
	// separate two applications run by one user.
	SeparationOwner
	// SeparationPeer: a service attests each record with what the kernel
	// reported about the connection. The only level at which "which app" is
	// a fact.
	SeparationPeer
)

func (s Separation) String() string {
	switch s {
	case SeparationOwner:
		return "owner"
	case SeparationPeer:
		return "peer"
	default:
		return "none"
	}
}

// Separated is implemented by sinks that can say what they enforce. A sink
// that cannot answer is assumed to enforce nothing.
type Separated interface {
	Separation() Separation
}

func SeparationOf(s Sink) Separation {
	if sep, ok := s.(Separated); ok {
		return sep.Separation()
	}
	return SeparationNone
}
