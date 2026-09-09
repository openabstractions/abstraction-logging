package logging

import "os"

// Who is talking, and who says so.
//
// The first design here had two fields: Source, which the writer filled in, and
// Peer, which a service filled in from the kernel. That was better than one
// field and still wrong, because it assumed there are exactly two answers and
// that the second one settles it.
//
// There are as many answers as there are things willing to vouch, and a relay
// makes the difference matter. Send a record client → local service → NAS
// aggregator and the aggregator's SO_PEERCRED identifies THE LOCAL SERVICE, not
// the original process. With one verified field the second stamp overwrites the
// first and the real author disappears. With a chain, both are on the record and
// a reader can see that the kernel vouched for the client at hop one and for the
// relay at hop two — which is the truth, and is not expressible any other way.
//
// So identity is a list. Every hop appends what it can establish and destroys
// nothing, including claims it believes to be false: a lie is evidence, and the
// place to resolve it is the reader, which has context that the writer does not.
type Attestation struct {
	// By names the mechanism, not the party. "self" for the writer's own claim;
	// otherwise the system facility that answered — "so_peercred", "file_owner",
	// "peer_cert". A reader that does not recognise a mechanism must treat it as
	// unverified rather than guessing.
	By string `json:"by"`

	// Verified is true when something other than the subject established this.
	// A self attestation is never verified, by definition; it is recorded
	// because it is usually right and always informative.
	Verified bool `json:"verified"`

	// At which hop, counting from the emitter. Hop 0 is the process that wrote
	// the record. This is what makes a relay chain readable rather than a pile.
	Hop int `json:"hop"`

	Program string `json:"program,omitempty"`
	Host    string `json:"host,omitempty"`
	User    string `json:"user,omitempty"`
	Exe     string `json:"exe,omitempty"`

	// UID, GID and PID use -1 for "not established" so that zero keeps its real
	// meaning. uid 0 is root, and a provider that could not determine a uid must
	// not be indistinguishable from one that determined root.
	UID int `json:"uid"`
	GID int `json:"gid"`
	PID int `json:"pid"`
}

// Unknown is what an unestablished numeric identity looks like.
const Unknown = -1

// BySelf is the mechanism name for an unverified self-description.
const BySelf = "self"

// ByPeerCred is the kernel answering about a unix socket peer.
const ByPeerCred = "so_peercred"

// Claim builds the writer's own description of itself. It is hop 0, it is never
// verified, and it is worth recording anyway: almost nothing is lying, and when
// something is, the claim is the evidence.
func Claim(program string) Attestation {
	host, _ := os.Hostname()
	return Attestation{
		By: BySelf, Verified: false, Hop: 0,
		Program: program, Host: host,
		UID: Unknown, GID: Unknown, PID: os.Getpid(),
	}
}

// Identity is the ordered chain on a record.
type Identity []Attestation

// Claimed returns the writer's own description, if it made one.
func (id Identity) Claimed() (Attestation, bool) {
	for _, a := range id {
		if a.By == BySelf && a.Hop == 0 {
			return a, true
		}
	}
	return Attestation{}, false
}

// Verified returns the earliest verified attestation — the one closest to the
// actual author.
//
// Earliest, not strongest or latest, and that is the whole point of keeping the
// chain. In a relay the LAST verified attestation describes the relay; the first
// describes whoever originally spoke. A reader asking "who wrote this" wants the
// first, and a reader asking "who handed it to me" wants the last.
func (id Identity) Verified() (Attestation, bool) {
	best := Attestation{Hop: -1}
	found := false
	for _, a := range id {
		if !a.Verified {
			continue
		}
		if !found || a.Hop < best.Hop {
			best, found = a, true
		}
	}
	return best, found
}

// Relay returns the last verified attestation: whoever most recently handed this
// record on.
func (id Identity) Relay() (Attestation, bool) {
	best := Attestation{Hop: -1}
	found := false
	for _, a := range id {
		if a.Verified && a.Hop >= best.Hop {
			best, found = a, true
		}
	}
	return best, found
}

// By returns the attestation from a named mechanism.
func (id Identity) By(mechanism string) (Attestation, bool) {
	for _, a := range id {
		if a.By == mechanism {
			return a, true
		}
	}
	return Attestation{}, false
}

// Disputed reports whether anything verified contradicts what was claimed.
//
// This is the signal worth alerting on, and it only exists because nothing is
// discarded. A process claiming to be systemd while the kernel says it is uid
// 1024 running /usr/bin/curl is the interesting case, and both halves have to
// survive for anyone to notice.
func (id Identity) Disputed() bool {
	claim, ok := id.Claimed()
	if !ok {
		return false
	}
	for _, a := range id {
		if !a.Verified {
			continue
		}
		if a.Host != "" && claim.Host != "" && a.Host != claim.Host {
			return true
		}
		if a.PID != Unknown && claim.PID != Unknown && a.PID != claim.PID {
			return true
		}
	}
	return false
}

// Trusted reports whether anything other than the writer vouched for this.
func (r Record) Trusted() bool {
	_, ok := r.Identity.Verified()
	return ok
}

// An Attester answers "who is on the other end of this?" — and every useful
// implementation of it is one the operating system already operates.
//
// This package does not mint identity. It has no accounts, no tokens, no
// registration and no secrets, and adding any of them would be a mistake: an
// identity this project invented would have to be defended by this project, and
// would be exactly as trustworthy as a self-declared field. The whole value of a
// verified attestation is that something with more authority than us made it.
//
// So the bindings are all system mechanisms:
//
//	SO_PEERCRED / LOCAL_PEERCRED   the kernel, on a unix socket    (peer_linux.go)
//	named pipe client token        the Windows LSA                 (not written)
//	file ownership + mode          the filesystem                  (FileSink)
//	container uid mapping          the runtime                     (inherited)
//
// The same relationship the download layer has with BITS: the good
// implementation already exists, is maintained by people with more leverage than
// us, and the job is to expose it behind one interface rather than reimplement
// it. Where the OS offers nothing, the honest answer is an error — see
// errNoPeerCreds — not a plausible-looking guess.
type Attester interface {
	// Attest reports what the system says about the peer of this connection,
	// as an attestation at the given hop. It must fail rather than approximate:
	// an inferred identity is a claim wearing a better hat.
	Attest(conn any, hop int) (Attestation, error)
}

// Separation is what a sink can actually enforce, and the three answers are very
// different. Naming them stops "the service will handle it" from standing in for
// a design.
type Separation int

const (
	// SeparationNone: one file everyone writes to. Attribution is by claim only.
	// Fine for one user's own machine, useless the moment two accounts share.
	SeparationNone Separation = iota

	// SeparationOwner: one file per user, and the OS enforces who may write
	// which. Identity comes from filesystem ownership rather than from the
	// record, so a user cannot forge another user's lines — they cannot open the
	// file. Needs no service and no daemon.
	//
	// What it CANNOT do is separate two applications run by the same user. They
	// have identical credentials as far as the filesystem is concerned.
	SeparationOwner

	// SeparationPeer: a service accepts records over a local socket and attests
	// each with kernel-reported uid, gid, pid and executable. The only level at
	// which "which app" is a fact, and the only one that survives a hostile
	// local process.
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

// Separated is implemented by sinks that can say what they actually enforce.
// A sink that cannot answer is assumed to enforce nothing, which is the safe
// reading.
type Separated interface {
	Separation() Separation
}

// SeparationOf reports what a sink enforces.
func SeparationOf(s Sink) Separation {
	if sep, ok := s.(Separated); ok {
		return sep.Separation()
	}
	return SeparationNone
}
