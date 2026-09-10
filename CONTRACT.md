# Contract

Every rule this layer states, each carrying a tag, in the order they were
decided. A conformance scenario cites the tag it tests on its `# expect` line,
and a citation that resolves to no rule here is a defect in one of the two.
The scenarios are in `testdata/scenarios/`; what they can and cannot reach is
the last section of this page.

[README.md](README.md) is the door — what this layer is, how to obtain it, one
example that runs. No rule on that page carries a tag.

This page is small because the layer is. It states what a record looks like,
where one goes, and what a reader may believe about who wrote it. It does not
state a logging API, because the language already has one.

---

## What this is not, and why nothing here has a tag

Go has `log/slog`. Python has `logging`. Both are already the interface an
application codes against with handlers swapped underneath, and both are in the
standard library. A third would be a competitor to something already standard.

So there is no facade here and no rule about one. What the standard facades
cannot do is leave the process: an in-process handler cannot send a record to a
service, to another machine, or to a supervisor, and "what was the file server
doing with my download while the laptop was asleep" is a question no in-process
handler can answer. This layer supplies the two parts that are missing — a
record format several languages agree on, and a sink that outlives the process
writing to it — and nothing else.

Three things it therefore refuses, none of which is a rule because a rule is
something an implementation can break:

- **It mints no identity.** No accounts, no tokens, no registration. Every
  useful identity provider is one the operating system already operates, and
  an identity this layer invented would have to be defended by this layer and
  would be worth exactly as much as a self-declared field. The one key it
  handles is the operator's, installed on a relay the way a socket path is,
  and it names that relay to a receiver that already holds it — never a writer.
- **It does not define an instant.** A timestamp is the job layer's
  `Timestamp`, cited rather than copied. Two definitions of one instant existed
  here until 2026-09-06 and had already drifted in how they parsed.
- **It does not rotate or retain.** Measured: three days of logs against the
  weights on the same disk is four orders of magnitude apart, so disk wear is
  not an argument this layer answers.

---

## The record

One record is one line: a single JSON object, terminated by exactly one LF and
put on the wire in one write [LOG-R1]. That is what lets several processes on
several machines append to one file and interleave whole lines rather than
halves of two, and it is why an oversized record is shrunk rather than split.

Every record carries a schema number, and a reader that meets a number it does
not understand refuses the whole record rather than reading the fields it
recognises [LOG-R2]. These lines are the interface between programs that were
not built together, so a newer writer must not be silently misread by an older
reader.

**A level is a number, not a name** [LOG-R3]. The scale is the one `slog` uses —
debug -4, info 0, warn 4, error 8 — because names do not survive translation:
Python has no trace and no fatal, Go has no critical, and a name-based format
forces every binding to invent a mapping and get it subtly wrong. A number that
falls between two landmarks is named by the landmark below it and its distance
from it, `ERROR+4`, never rounded to the nearer landmark [LOG-R4]. A binding
whose language has a level this scale does not name places it between landmarks
and nobody has to agree on what to call it.

**An attribute value is a string** [LOG-R5]. A JSON number is how an int64
quietly becomes a float64 across a language boundary, and a log line that
silently rounds a byte count is worse than one that quotes it. An integer
arrives as its decimal digits.

**A group flattens to a dotted key** [LOG-R6]. A nested object is the obvious
alternative and the wrong one: the value of a shared sink is that a search for
one key works, and dotted keys keep every value on the line that mentions it.

**A timestamp is fixed width** — six fractional digits always, and the zone
designator `Z` [LOG-R7]. Trailing zeros are not trimmed. One layer shipped a
cross-language defect here because one implementation trimmed them and another
did not, and both cited the same instant.

**A line is UTF-8, and text shortened to fit a cap is cut on a character
boundary** [LOG-R8]. Slicing at a byte index splits a character, and a JSON
string carrying half of one is not JSON. The damage is quiet in a different way
in each binding: one encoder substitutes a replacement character and writes a
corrupted message that still parses, another writes bytes no reader can decode,
and a reader comparing the two sees a format disagreement rather than a
shortened message.

Anything a particular logger knows that this record does not name travels in
the attributes. No field is added for it, which is what lets one layer learn
something new without every other binding changing shape.

---

## Where a record goes

**Where a record goes is a property of the machine, not a choice of the code
that logs** [LOG-S1]. An application says "log". It names no path, no host and
no service, and the same call answers differently on two machines because the
two machines offer different things.

The chain is a service, then a file, then nothing, in that order [LOG-S2]. A
service can ask the kernel who sent a record and get an answer the sender cannot
influence; a file can only be owned. Nothing above the chain chooses which rung
it lands on.

**A machine that offers nothing gets a working sink that discards, never an
error** [LOG-S3]. A library that logs must not fail because the application
declined to configure logging, and it must not decide on the application's
behalf where the logs go.

**A configured sink that cannot be opened degrades to the rung below it and
says so once**, on the standard error stream, and the program starts [LOG-S4].
Refusing to run because a log file could not be opened would make logging the
most dangerous component in the system.

**A service that cannot be reached degrades per record, not per process**
[LOG-S5]. The record goes to the rung below; the caller is not blocked and
nothing retries inside its log statement, because the moment a program is
logging heavily is the moment something is already going wrong. A service
configured with nothing beneath it discards while it is unreachable, which is
the honest reading of a chain whose last rung is nothing.

**A record over a sink's line cap is shrunk until it fits and marked as shrunk**
[LOG-S6]. It is never dropped and never split across two writes: dropping loses
the event most likely to matter, the one carrying a giant payload, and splitting
breaks the one property that lets several writers share a file.

**Shrinking is a finite ladder, and every step narrows the record** [LOG-S9].
The message is not the only thing on a record that can be too long: identity and
job travel with it, and where the retained metadata alone exceeds the cap there
is no shorter message to find — a one-character message does not halve, and an
empty one cannot shrink at all. So a step either shortens the message or drops
one named piece of metadata; a step that would not make the encoded record
strictly smaller is not taken; and what was dropped is named on the line beside
the shrink mark. A shrink that can produce a size it has already produced is a
sink that never returns, which is worse than any line it could have written.

**A cap no record can fit under is refused, and nothing is written** [LOG-S10].
Below the smallest line this layer can encode — a schema number, an instant, a
level and the shrink mark — there is no record that both fits and is a record,
and writing the oversized line anyway would break `LOG-R1` for every other
writer sharing the file. The refusal is distinguishable from a failure to write,
because a cap too small is something the application configured and a failure to
write is something the disk did. This is the only size a sink refuses; every
other oversized record shrinks.

**A sink that fans out to several does not let one failure stop the others**
[LOG-S7]. A record reaching two of three places beats an exception in the
caller's hot path.

**The handler a program installs is heard even when the machine offers nothing**
[LOG-S8]. Where the chain lands on nothing, the default handler is the
language's own text output on the standard error stream rather than a discard:
a daemon nobody configured that says nothing at all is a defect in this layer,
and two daemons carried that fallback themselves before it lived here.

---

## Who is talking, and who says so

Attribution is two problems, not one: what a process says about itself, and what
something else establishes about it. Keeping them in one field assumes there are
exactly two answers and that the second settles it, and a relay makes both
assumptions false — a record travelling client, local service, aggregator is
identified by the aggregator's kernel as *the local service*, and the original
author disappears.

**So identity is an ordered chain, one attestation per hop, counting from the
writer** [LOG-I1]. Hop 0 is the process that wrote the record; hop *k* is what
the *k*-th receiver established about the party that handed it the record. The
subject of hop 1 is the writer; the subject of every later hop is a relay.

**The writer's own attestation names the mechanism `self` and is never
verified** [LOG-I2]. It is recorded anyway: almost nothing is lying, and when
something is, the claim is the evidence.

**An attestation names the mechanism that answered, never the party** [LOG-I3].
A reader that does not recognise a mechanism treats the attestation as
unverified rather than guessing what it meant.

**Nothing is ever removed from the chain** [LOG-I4]. A claim survives every
contradiction, including one this layer believes to be false, because the
contradiction is the signal worth alerting on and this is not the layer with
enough context to adjudicate it.

### What the wire can say, and what it cannot

An attestation's `verified` field is the assertion of the party that appended
it: *I established this rather than copied it*. On the wire a genuine stamp and
a forgery are the same bytes. So **an upstream party's assertion that it
verified something is evidence received by a receiver, not verification
established by it: a wire-level `verified` is preserved as an attributed claim
and stands, to the receiver, as an assertion at most** [LOG-I5]. Nothing is
stripped on send or on accept — the earlier form of this rule stripped every
verified attestation at both sites, which made a two-hop chain unreachable and
a relay's genuine stamp indistinguishable from a forgery by destroying both.
~~A verified attestation a party attached to its own record does not survive
being sent; it is stripped before the record leaves the process and again by
the party that accepts it.~~ Struck 2026-09-09: it answered a forgery by
deleting the evidence and the genuine article alike.

**A receiving service appends what it independently established about its
immediate peer, as a new hop, and overwrites nothing** [LOG-I6]. The hops in
front of it are still there, whatever they assert.

**Claimed verification is kept separate from the receiver's trust assessment.**
What a hop is worth to a reader is its *standing*, and there are four:

| standing | what it means to the assessor |
| --- | --- |
| `claimed` | the hop asserts no verification: the writer's own claim |
| `asserted` | the hop asserts it was verified, and nothing the assessor trusts stands behind that. A forged stamp and a genuine stamp from an unauthorised relay both land here |
| `vouched` | verified through an explicit mechanism the assessor holds — an authorised relay on a trusted path, or evidence bound to the record |
| `established` | the assessor made this attestation itself |

The first two read the wire; the last two are the assessor's own conclusion.
This is RFC 8601's shape — an `Authentication-Results` field records the checks
its own boundary ran, and a result that arrived from outside that boundary is a
claim however it is labelled — and RFC 9440's for a forwarded client
certificate: an origin believes the `Client-Cert` field only from a proxy it
has authenticated. The one declared divergence from RFC 9440 §2.4 is that a
receiver here does not sanitise the incoming field away; it keeps it, at its
standing, because `LOG-I4` and the reader's need for the evidence outrank the
tidiness.

**The assessment starts from an anchor the assessor holds outside the record —
the attestation it established itself — and never from anything read off the
wire; with no anchor, no hop stands above `asserted`** [LOG-I13]. This is the
trust anchor of RFC 5280 §6.1.1, delivered out of band. A receiving service's
anchor is the stamp it just appended; a reader of a file no service wrote has
none, and gets claims back, which is exactly what a file can attest.

**From the anchor the path is walked downward, and a hop stands `vouched`
through an authorised relay only when the trusted hop above it describes a
party the assessor's policy authorises to attest the hops beneath it**
[LOG-I14]. Being on the path is not being allowed to vouch: this is the `cA`
basic constraint of RFC 5280 §4.2.1.9, and certification path validation, §6,
is the walk. A genuine stamp carried by a relay the policy does not name stands
`asserted`, the same as a forgery — the receiver has no basis to tell them
apart, and says so rather than guessing.

**A hop bound to the record under a key the assessor holds stands `vouched`
with no path at all; a binding that fails — a key the assessor does not hold,
or a record altered after the binding — stands `asserted`** [LOG-I15]. The
binding covers the record's content and the chain up to and including the bound
hop, so a message, an attribute, a level, the writer's claim or the bound
subject changed afterwards breaks it. DKIM (RFC 6376) is the ancestor: evidence
travelling with the message, verifiable without trusting what carried it. The
declared divergence is that the key is symmetric, HMAC-SHA256, because every
platform furnishes it and the assessor that holds the key is the trust root of
its own sink; a verifier can therefore forge what it verifies, and this is not
the mechanism for a reader that must not be trusted with that. The bound bytes
are the record as canonical JSON — keys sorted, no HTML escaping, no trailing
newline — which is RFC 8785's shape for the ASCII this layer writes and is
**unproven against a second language** beyond it.

**The author is the attestation about the writer — hop 1 — and only when the
path from the anchor reaches it. If only the immediate peer is established, the
author is unverified** [LOG-I7]. ~~The earliest verified attestation is the
author — whoever originally spoke.~~ Struck 2026-09-09: the earliest hop
asserting verification is not the author, it is a claim about the author, and
reading it as the author is precisely how a forged earlier hop is promoted.

**The relay is the assessor's immediate peer: the hop it established itself**
[LOG-I8]. ~~The last verified attestation is the relay.~~ Struck the same day,
for the same reason: last on the wire is where a forger writes. In a chain of
one, author and relay are the same answer, and the distinction is the whole
reason the chain is kept.

**A record nothing has vouched for is unattributed, and says so** [LOG-I9]. It
is not read as the writer's claim promoted for want of anything better.

**A dispute compares claims about one subject: the writer's claim against the
attestation about the writer, and only when that attestation stands `vouched`
or `established`; both halves stay on the record** [LOG-I10]. A process
claiming to be the init system while the kernel reports an ordinary account
running an ordinary binary is the interesting case. A relay carrying a
different pid or host from the writer is expected, not a dispute; and a
contradiction the receiver has no basis to trust is an unverified author, which
is a different answer.

**An unestablished numeric identity is -1, never 0** [LOG-I11]. Zero keeps its
real meaning: a provider that could not determine a user id must not be
indistinguishable from one that determined the superuser.

**Where the platform's attester cannot answer, the record records that it could
not** [LOG-I12]. The line stays unattributed for a stated reason rather than
looking unattributed for an unknown one.

### What this design cannot refuse, stated

An authorised relay can forge anything beneath it, the way a certification
authority can. Authorisation is the policy's statement that it accepts that.
And a relay that is authorised by identity is authorised for the hops it
carries on a channel the receiver's kernel mediated; a record that passes
through a party the receiver does not trust, or rests in a store between the
stamp and the reader, needs the bound evidence of `LOG-I15`, and this layer has
no transport yet on which that case arises.

---

## What a sink separates

Naming this stops "the service will handle it" from standing in for a design.

**A sink says what it actually enforces, and there are three answers** [LOG-P1]:

| level | enforces | cannot do |
| --- | --- | --- |
| `none` | nothing. Attribution is by claim only | anything |
| `owner` | one file per account, enforced by the operating system | separate two applications run by one account |
| `peer` | the kernel's own answer about the connection: account, process, image | — |

A sink that cannot answer enforces nothing, which is the safe reading.

**`owner` is read from the mechanism the filesystem is enforcing, not from the
path convention** [LOG-P2]. A per-account path is advice; the mode on disk is
the mechanism, and a world-writable file separates nothing however carefully its
name suggests otherwise.

**Where a platform's access control cannot be read through this interface, the
sink reports `none`** [LOG-P3]. That is an admission rather than a policy: a
mode check on a platform that enforces with access-control lists would be
answering a question about a number that platform never set, and anything
deciding on separation must refuse rather than assume.

**A sink reports the separation the running platform can actually enforce, never
the separation its mechanism enforces elsewhere** [LOG-P4]. **This rule is not
kept today.** A sink that forwards to a service reports `peer` on every
platform, while the peer-credential lookup behind it answers on one and returns
an error on the others — so on a platform with no attester the sink promises a
separation nothing can deliver and every record it forwards is unattributed.
The rule is stated because it is the rule; the gap is stated because a reader
deciding a permission model on this answer would be wrong today.

---

## What a conformance run can reach

One driver exists, in Go, at `go/cmd/replay`, and the seven scenarios in
`testdata/scenarios/` are green against it on Windows and on Linux. Every rule
below is therefore **proven against one implementation and UNPROVEN against any
second**, which for a layer whose claim is that several languages agree on one
record format is the gap that matters: a green against one binding proves that
it agrees with itself.

**Reachable by a driver alone**, needing nothing but the machine it runs on.
Thirty-two rules, seven scenarios: the shape of one encoded record, a reader
refusing a schema, the delegation chain and each way it degrades, an oversized
record shrinking, a record whose retained metadata alone will not fit, a cap too
small for any record at all, a message of three-byte characters cut to fit
without splitting one, the identity chain along a real two-hop path, a
contradiction that must survive, and three attacks on the chain — a forged earlier hop, an
unauthorised relay, evidence altered after it was bound — plus a forged
binding, none of which is promoted.

**Declared and reached by no scenario**, five rules, each for a stated reason:

| rule | why no scenario reaches it |
| --- | --- |
| `LOG-S7` | fanning out to several sinks needs a failing sink the vocabulary has no way to build, and inventing one for a rule this small buys a green nobody needed |
| `LOG-I3` | a naming convention for mechanisms. A driver that named parties instead would still answer with tokens, and no expectation could tell which it meant |
| `LOG-P2` | the mode is enforced on some platforms and not on others, so the same scenario would demand two different answers |
| `LOG-P3` | reachable only on a platform whose access control this interface cannot read, which is exactly the platform where the answer is a constant |
| `LOG-P4` | as above, and it is the rule this layer does not keep |

**Reached against a benched chain, and separately against a kernel.** The
scenarios hand the driver what a platform's attester answered, so the
arithmetic of the chain is judged on any machine. The same path — a writer, a
relay and a receiver on three unix sockets, the kernel stamping both links — is
run by this binding's own tests on Linux, and nowhere else: the peer-credential
lookup answers on one platform of three, and on the other two the service
records that it could not attest and every record it accepts is unattributed
with that reason.

**Not reachable by any run, in any language but one.** One binding exists.
Until a second one runs these scenarios, a green proves that an implementation
agrees with itself.

**What the shared runner still needs.** The scenarios name a capability and its
operations, and the driver contract page does not yet carry this layer's
section: its verdicts, the shape of its answers, and the operations the scenario
files use. Until it does, a stranger cannot write a conforming driver from the
published pages alone, and that is a defect in the pages rather than in the
scenarios.

---

## What was tested where

| | ran | did not run |
| --- | --- | --- |
| the scenarios | against this binding's driver on a Windows workstation and on Linux under WSL2, every cited rule shown to fail when broken in the implementation | against any second language, on any platform |
| the record format | one language's own unit tests | any second language — the second binding is an empty directory |
| the identity chain | the walk, the binding and each attack in unit tests on both platforms; the real two-hop socket path on Linux | the two-hop path on Windows or macOS, where the kernel lookup is not written |
| peer credentials | one kernel's own unit tests, in one language | every other platform; the lookup returns an error there |
| the service | its unit tests, and as a real listener under the driver for the tier scenario | as a shipped daemon. Nothing ships one, and nothing this project runs writes where a reader could see |
