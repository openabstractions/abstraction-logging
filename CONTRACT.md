# abstraction.logging contract

Remote profile: the current service methods retain `local` (explicit or
default); the general paired listener does not offer this capability. Retained
capability-specific delegation keeps its existing contract and is separate
from that listener. Held by: `idl/gen/service_test.go`
(`TestDispatcherTableCarriesRemoteProfile`) and `serve/runtime_remote_test.go`
(`TestRemoteRuntimeDescribeUsesLiveGrantAndExplicitServices`).

Binds: `logging.thrift`

`logging.thrift` defines `abstraction.logging/sink@1`, the record-submission
service every writer targets, and the retained-history services
`abstraction.logging/reader@1` and `abstraction.logging/observer@1`, which
read what a provider kept. This page states what a record looks like, where
one goes, what a reader may believe about who wrote it, and what the history
services promise about the pages they return. It does not state a logging
API, because the language already has one. [README.md](README.md) is the
door — what this module is, how to obtain it, one example that runs.

## Reading this page

The conventions every OA contract carries are stated once in [CONVENTIONS.md](https://github.com/openabstractions/abstractions/blob/main/idl/CONVENTIONS.md); a rule here that applies one cites its `CONV-` id.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY" and "OPTIONAL" in this
page are to be interpreted as described in BCP 14 (RFC 2119, RFC 8174) when,
and only when, they appear in all capitals, as shown here.

A rule id such as `LOG-R1` is declared once, in bold brackets before its
title, at the head of the rule it names; a citation that resolves to no rule
here is a defect in one of the two. Tests and refusals cite it the same way,
`[LOG-R1]`. A retired id is never reused; [HISTORY.md](HISTORY.md) keeps it
with the release it left. `HISTORY.md` also carries this contract's prior
art and what was measured or tested, linked from here and linking back. The
scenarios are in `testdata/scenarios/`.

| letter | meaning |
| --- | --- |
| R | record — the shape of one encoded line |
| S | sink — where a record goes and how delivery degrades |
| I | identity — the attestation chain and who may be believed |
| P | pages — what a sink attests it separates, and the retained-history reader's bounded pages |
| N | native — the operating system's own log sources and their protected pages |

`A` (admission) and `E` (error and outcome) are reserved with one meaning in
every contract (S12) and are not separately declared here: every rule below
states its own admission and outcome inline. `X` (extension) is unused.

This release renames no wire field or method of `sink@1`, `reader@1` or
`observer@1`; Reading this page carries no prose-to-wire table.

## Rules

### [Record](https://openabstractions.github.io/reference.html#concept-log.record)

**[LOG-R1] One record, one line.** A record MUST be a single JSON object,
terminated by exactly one LF, put on the wire in one write. That is what
lets several processes on several machines append to one file and
interleave whole lines rather than halves of two, and it is why an
oversized record is shrunk rather than split.

**[LOG-R2] Unknown schema refused whole.** Every record MUST carry a schema
number. A reader that meets a number it does not understand MUST refuse the
whole record rather than reading the fields it recognises. These lines are
the interface between programs not built together. A newer writer's record
must not be silently misread by an older reader.

**[LOG-R3] A level is a number, not a name.** The scale is the one `slog`
uses — debug -4, info 0, warn 4, error 8 — because names do not survive
translation: Python has no trace and no fatal, Go has no critical, and a
name-based format forces every implementation to invent a mapping and get
it subtly wrong.

**[LOG-R4] Named by distance, never rounded.** A number that falls between
two landmarks MUST be named by the landmark below it and its distance from
it, `ERROR+4`. The service MUST NOT round it to the nearer landmark. An
implementation whose language has a level this scale does not name places
it between landmarks and nobody has to agree on what to call it.

**[LOG-R5] An attribute value is a string.** An attribute value MUST be a
string. A JSON number is how an int64 quietly becomes a float64 across a
language boundary, and a log line that silently rounds a byte count is
worse than one that quotes it. An integer arrives as its decimal digits.

**[LOG-R6] A group flattens to a dotted key.** A nested value MUST be
flattened to a dotted key. A nested object is the obvious alternative and
the wrong one: the value of a shared sink is that a search for one key
works, and dotted keys keep every value on the line that mentions it.

**[LOG-R7] A timestamp is fixed width.** A timestamp MUST carry six
fractional digits always, with the zone designator `Z`. Trailing zeros MUST
NOT be trimmed. One module shipped a cross-language defect here because one
implementation trimmed them and another did not, and both cited the same
instant.

**[LOG-R8] UTF-8, cut on a character boundary.** A line MUST be UTF-8, and
text shortened to fit a cap MUST be cut on a character boundary. Slicing at
a byte index splits a character, and a JSON string carrying half of one is
not JSON. The damage is quiet in a different way in each implementation:
one encoder substitutes a replacement character and writes a corrupted
message that still parses, another writes bytes no reader can decode, and a
reader comparing the two sees a format disagreement rather than a shortened
message.

**[LOG-R9] [Record](https://openabstractions.github.io/reference.html#concept-log.record) content is data.** The service, every provider, every
bridge handler and every reader MUST treat the message, the job and every
attribute as inert text. No lookup, substitution, template expansion with
external resolution, deserialization into objects or evaluation happens on
any of them. A record carrying `${jndi:ldap://x/a}`, `%n`, `{0}` or `$(id)`
reads back byte-identical. Log4Shell and its follow-up CVEs came from
message lookups inside a logging library.

Anything a particular logger knows that this record does not name travels
in the attributes. No field is added for it, which is what lets one module
learn something new without every other implementation changing shape.

### [Sink](https://openabstractions.github.io/reference.html#concept-log.sink)

Two kinds of sink answer this question: the **service sink**, resolved
through the facade, and the **local sink**, the environment chain a
provider selects by name. The service sink is the logging capability an
application adopts: Go [Default](https://openabstractions.github.io/reference.html#concept-logging.default-handler), `Resolve` and `NewResolvedHandler`, and
each language's `resolve_log`. `LOG-S11` governs its absent service, and
`LOG-S12` and `LOG-S13` a service that disappears after binding. The local
sink is Go `LegacyAuto` and `LegacyDefault`; `LOG-S1` to `LOG-S5` and
`LOG-S8` describe it. The remaining rules on this page apply to the `Sink`
interface either one implements.

**[LOG-S1] Machine property, not code choice.** Where a record goes MUST be
a property of the machine, not a choice of the code that logs. An
application says "log". It names no path, no host and no service, and the
same call answers differently on two machines because the two machines
offer different things.

**[LOG-S2] Service, then file, then nothing.** The chain MUST be a service,
then a file, then nothing, in that order. A service can ask the kernel who
sent a record and get an answer the sender cannot influence; a file can
only be owned. Nothing above the chain chooses which fallback sink it lands
on.

**[LOG-S3] Discards, never errors.** A machine that offers nothing MUST get
a working sink that discards, and MUST NOT get an error. A library that
logs MUST NOT fail because the application declined to configure logging,
and MUST NOT decide on the application's behalf where the logs go.

**[LOG-S4] Degrade and say so once.** A configured sink that cannot be
opened MUST degrade to the fallback sink below it and MUST say so once, on
the standard error stream, and the program MUST start. Refusing to run
because a log file could not be opened would make logging the most
dangerous component in the system.

**[LOG-S5] Per record, not per process.** A service socket on the
environment chain that cannot be reached MUST degrade per record, not per
process. The record goes to the fallback sink below; the caller is not
blocked and nothing retries inside its log statement, because the moment a
program is logging heavily is the moment something is already going wrong.
A service configured with nothing beneath it discards while it is
unreachable, which is the honest reading of a chain whose last fallback
sink is nothing. The degradation belongs to the chain, a provider the
application selected, and in Go it is the named `LocalFallbackSink`. The
local sink's socket fallback sink alone, Go `ServiceSink`, returns the
failure to reach its service as the write's error. A retry lives in the
service sink's asynchronous delivery loop (`LOG-S12`), which runs after the
log statement that queued the record has returned.

**[LOG-S6] Shrunk, never dropped or split.** A record over a sink's line
cap MUST be shrunk until it fits and MUST be marked as shrunk. It MUST NOT
be dropped and MUST NOT be split across two writes: dropping loses the
event most likely to matter, the one carrying a giant payload, and
splitting breaks the one property that lets several writers share a file.

The additive Go owned-audit path `EncodeAttributedExact`,
`WriteAttributedExact` and `WithExactEmitter` accepts a trusted explicit
physical-row cap and preserves every supplied value. It is restricted to the
existing OS-locked, single-writer retained provider. A row exceeding that cap
returns `ErrLineCap` without writing; it never enters this shrink ladder.
The complete-row encoder, including private emitter evidence and newline, is
the shared size-check and append authority. A successful size check reserves
no storage, and an I/O failure may leave a partial append. This path supplies
no large-write atomicity promise for shared files or multiple writers.
Ordinary `Write`, `WriteAttributed`, `WithEmitter` and `MaxLine` retain their
existing behavior and published Go signatures.

**[LOG-S9] Shrinking is a finite ladder.** Every step MUST narrow the
record. The message is not the only thing on a record that can be too
long: identity and job travel with it, and where the retained metadata
alone exceeds the cap there is no shorter message to find — a
one-character message does not halve, and an empty one cannot shrink at
all. A step either shortens the message or drops one named piece of
metadata. A step that would not make the encoded record strictly smaller is
not taken, and what was dropped is named on the line beside the shrink
mark. A shrink that can produce a size it has already produced is a sink
that never returns, which is worse than any line it could have written.

**[LOG-S10] Below the floor, refused whole.** A cap no record can fit under
MUST be refused, and nothing MUST be written. Below the smallest line this
module can encode — a schema number, an instant, a level and the shrink
mark — there is no record that both fits and is a record, and writing the
oversized line anyway would break `LOG-R1` for every other writer sharing
the file. The refusal is distinguishable from a failure to write, because a
cap too small is something the application configured and a failure to
write is something the disk did. This is the only size a sink refuses;
every other oversized record shrinks.

**[LOG-S7] Fan-out isolates failures.** A sink that fans out to several
MUST NOT let one failure stop the others. A record reaching two of three
places beats an exception in the caller's hot path.

**[LOG-S8] Heard even when the machine offers nothing.** The handler a
program installs from the environment chain MUST be heard even when the
machine offers nothing. The rule covers the environment-chain default,
which in Go is `LegacyDefault` over the chain `LegacyAuto` reads. Where
that chain lands on nothing, the default handler is the language's own text
output on the standard error stream. A daemon nobody configured that says
nothing at all is a defect in this module, and two daemons carried that
fallback themselves before it lived here. The service-resolved default, Go
`Default`, reports an absent runtime as the resolution error of `LOG-S11`.
Its fallback is the native logger that the adopter installs explicitly
beside the facade (`LOG-S15`).

**[LOG-S11] Absent service is the resolution error.** A service sink MUST
report an absent service as the facade's resolution error, and the record
MUST go to no other sink than the native logger of `LOG-S15`. An application
that adopted logging requires the
service, as a program using a graphics API requires its driver (owner
ruling, 2026-09-16). When no runtime is installed or reachable, or the
runtime offers no logging sink, the eager resolution fails with the
resolution error: `runtime_unavailable` or the resolver's refusal, with the
capability, the contract and what was looked for. A write queues its record
at once in the bounded queue of `LOG-S12`, and the log statement never waits
on resolution or on a runtime start. The queue binds in the background,
retries with `LOG-S12`'s backoff and reports the error through `LOG-S13`.
A stopped installed runtime is started in the background, one start at a
time; after a failed start the next waits 5 s, doubling up to 1 min.
Closing a sink that never bound tries to bind once more within the caller's
deadline; when that fails, every queued record is counted failed and close
returns the resolution error. Writing the
record to the standard error stream, a file or a discarding sink in its
place would hide the missing runtime from the operator who enabled
logging, and the ruling names that substitution a defect. A fallback is the
local sink the application selects by name: the environment chain above,
or the native logger an adopter installs beside the facade (`LOG-S15`).

**[LOG-S12] Bounded queue, backoff, rebind.** A bound service sink whose
service disappears MUST keep its records in a bounded queue, MUST retry
them with backoff, and MUST rebind through the facade; the application
keeps running and the records go to no other sink. The runtime is a
supervised process, and a service that disappears after binding is that
process restarting at the same endpoint within seconds (decision
2026-09-16, `research/logging-sink-loss/DECISION.md`). Each language's
asynchronous handler holds the policy:

- The queue holds 1024 records by default, counting the record being
  delivered. A record leaves the queue when it is delivered.
- When the queue is full, the newest record is dropped and counted, and the
  log statement returns at once. No log statement waits on the service.
- A failed delivery keeps its record at the head of the queue. The handler
  rebinds at once: it selects the installed runtime again, checks its trust
  again and resolves `abstraction.logging/sink@1` again. It then retries
  after waiting 100 ms, doubling after each failed retry up to 5 s, each
  wait varied by up to 20% either way, and rebinds again before each retry.
  A handler over a client its application resolved rebinds through the
  resolve function the application supplied, and one given none retries the
  same client.
- Each delivery is bounded by a write timeout, 5 s by default. A delivery
  that times out is a failed delivery.
- Closing drains the queue until the caller's deadline, or for 5 s when
  the caller gives none. The record being
  delivered at the deadline is counted failed, and the records behind it
  are counted abandoned.
- Records go to no disk spool and no other sink. Durability belongs to the
  service's history, and a local runtime that relays to a remote service
  buffers in that history.

An application that must not run without its audit trail reads the
handler's `failing` state and stops itself.

**[LOG-S13] Four typed ways to see the loss.** A handler that loses its
service MUST make the loss visible in four typed ways.

- **Counts.** Every handler exposes `accepted`, `dropped`, `written`,
  `failed`, `abandoned`, `queued`, `failing` and `last_error` in its
  language's idiom. Every record handed to the handler is exactly one of
  accepted or dropped. Every accepted record ends as exactly one of
  written, failed or abandoned, or is still queued.
- **Transitions.** `on_failure(error, counts)` runs when a delivery fails
  after the previous one succeeded, once the first rebind attempt has
  returned. `on_recovery(counts)` runs when a retry reaches the service
  again, and `on_overflow(counts)` on the first dropped record since the
  queue was last empty. Each runs once per run of its condition, on the
  thread that observed it, before the handler continues. The error is the
  typed resolution error when that rebinding failed, and the delivery's
  error otherwise.
- **Default reporter.** A handler given none of the three callbacks writes
  one line per transition on its language's error stream:
  `abstraction.logging: service unreachable (<class>); <queued> queued, <dropped> dropped`,
  `abstraction.logging: queue full, dropping newest records; <queued> queued, <dropped> dropped`
  and `abstraction.logging: delivering again; <dropped> dropped`. The class
  is the resolution status when rebinding failed, `timeout` when the
  delivery timed out, and `write_failed` otherwise. The line reports the
  sink's state and MUST NOT carry a record.
- **Gap record.** A retry after a failure first delivers one record at
  level WARN, message `records dropped while the sink was unreachable`,
  with the attributes `dropped`, `since` and `until` (fixed-width instants,
  `LOG-R7`) and the handler's hop 0 claim. Its delivery is the recovery. It
  is not counted as accepted or written, and the queued records follow it
  in order.

  Every drop of an outage is reported by one gap record. An outage's drops
  are the records dropped from the last delivery before its failure until
  the queue is first empty after its recovery. `dropped` counts the
  outage's drops that no delivered gap record reported, up to the moment
  the gap record is built. Records dropped later, while that record is
  delivered or while the queue drains, go to a later gap record. The first
  time the queue is empty after the recovery, a handler holding unreported
  drops delivers another gap record with their count before it takes the
  next record. A failure before then carries them into that failure's gap
  record. `since` is when the failure began for an outage's first gap
  record, and the previous gap record's `until` for a later one; `until`
  is when the record was built. Once the queue has drained, the `dropped`
  attributes of an outage's delivered gap records sum to its drops. A
  close that reaches its deadline first leaves the rest unreported, as it
  leaves records abandoned.

**[LOG-S14] History retention.** The history MUST be retained under
`logs_max_use` and `logs_max_retention` (`research/storage-classes/DECISION.md`
§5, §8). The Go service-owned provider rotates `records.jsonl` monthly and
by size, removes expired completed segments by their newest write time,
and removes the oldest completed segments to meet the encoded-byte limit
(`go/retention.go`). The active writer remains protected under the same
mutex. The limit counts the active JSONL file and completed JSONL segments;
filesystem metadata and allocation rounding are outside it. `logs_max_use`
defaults to 10% of the file system capped at 4G and `logs_max_retention`
is unset by default, journald's own defaults for `SystemMaxUse` and
`MaxRetentionSec`. The reader's existing `gap` outcome is the only new
observable: a continuation whose unread records were removed returns `gap`,
the same outcome a reopened or truncated file already returns, and the caller
restarts from the empty cursor. Open, write and read enforce retention; hosts
collect daily and may call the native `Collect` under storage pressure. A
record larger than `logs_max_use` is refused by the provider and reported by
the host's existing error callback. Collection failures are reported and
prevent writes exceeding the bound. The runtime and standalone logging host
select the accepted default; native providers may supply byte/age policy.
Shared append-only `OpenFileSink` and adopter-owned log files keep their
existing lifecycle. No durability receipt or record schema change is
introduced. The person's controls over the retention keys are under
[Next](#next).

**[LOG-S15] Native logger when no service is offered.** A service sink
given the native logger an adopter installs beside the facade MUST send its
records to that logger while the machine answers that it offers no logging
service before its first binding, and only then (owner decision,
2026-09-15). The answer is one of:
no trusted runtime installation, a platform no runtime supports, or the
resolver's `unavailable`. Every other outcome stays the resolution error of
`LOG-S11`: `forbidden`, `incompatible`, `not_ready`, a trust failure, a
resolver timeout or transport error, a failed runtime start, and a write
failure after a successful binding.

- The native logger is the only switch, and the facade ships none.
- The native logger receives each record through the language's own
  logging: time, level, message and attributes. The identity chain stays
  behind, because no receiving service stamps it.
- On each transition into the fallback the native logger receives one WARN
  line, `abstraction.logging/sink@1 not offered: <class>`, with the class of
  `LOG-S13`. The transition back writes one line,
  `abstraction.logging/sink@1 offered again; records go to the service`.
- The machine is asked again at most once per 30 s, and a runtime installed
  later takes over without a restart. Records the native logger took are
  not replayed to the service.
- The handler's counts gain `native`: every accepted record ends as exactly
  one of written, native, failed or abandoned, or is still queued.

Go `DefaultWithNative(program, native slog.Handler)` and Python
`resolved_handler(native=logging.Handler)` implement it. Python reads no
installation as `abstraction.ipc` finding no native client and no recorded
installation, or, on Windows, default discovery's `runtime_unavailable` with
no installation recorded in the registry.

### Identity

Attribution is two problems, not one: what a process says about itself, and
what something else establishes about it. Keeping them in one field assumes
there are exactly two answers and that the second settles it, and a relay
makes both assumptions false — a record travelling client, local service,
aggregator is identified by the aggregator's kernel as *the local service*,
and the original author disappears.

**[LOG-I1] Identity is an ordered chain, one attestation per hop, counting
from the writer.** Hop 0 is the process that wrote the record; hop *k* is
what the *k*-th receiver established about the party that handed it the
record. The subject of hop 1 is the writer; the subject of every later hop
is a relay.

**[LOG-I2] Self-claimed, never verified.** The writer's own attestation
MUST name the mechanism `self`. A reader MUST NOT treat it as verified. It
is recorded anyway: almost nothing is lying, and when something is, the
claim is the evidence.

**[LOG-I16] Unclaimed hop for an unattributed arrival.** A record that
reaches a receiving service with no chain MUST get an explicit unattributed
hop 0, `unclaimed`, in front of the service's stamp. The hop MUST NOT be
verified, and carries -1 for every numeric identity and nothing else, and
the record gains the attribute `logging.writer_claim = absent`. Hop 0
belongs to the writer (`LOG-I1`); without this hop the service's stamp
would sit at hop 0 and the attestation about the writer would never be hop
1 (`LOG-I7`). Refusing the record would drop a line because its writer said
nothing about itself, which `LOG-S3` and `LOG-S6` do not allow a sink to
do. The inserted hop says why the record is unattributed (`LOG-I9`,
`LOG-I12`), it is not the writer's claim (`LOG-I2`), and a chain the writer
did send is kept as it arrived (`LOG-I4`).

**[LOG-I3] Mechanism, not party.** An attestation MUST name the mechanism
that answered, and MUST NOT name the party. A reader that does not
recognise a mechanism treats the attestation as unverified rather than
guessing what it meant.

**[LOG-I4] Nothing is ever removed from the chain.** A claim MUST survive
every contradiction, including one this module believes to be false,
because the contradiction is the signal worth alerting on and this is not
the module with enough context to adjudicate it.

#### What the wire can say, and what it cannot

An attestation's `verified` field is the assertion of the party that
appended it: *I established this rather than copied it*. On the wire a
genuine stamp and a forgery are the same bytes.

**[LOG-I5] An upstream party's assertion that it verified something is
evidence received by a receiver, not verification established by it.** A
wire-level `verified` MUST be preserved as an attributed claim, and stands,
to the receiver, as an assertion at most. Nothing is stripped on send or on
accept — the earlier form of this rule stripped every verified attestation
at both sites, which made a two-hop chain unreachable and a relay's genuine
stamp indistinguishable from a forgery by destroying both.

**[LOG-I6] A receiving service appends what it independently established
about its immediate peer, as a new hop, and overwrites nothing.** The hops
in front of it are still there, whatever they assert.

**Claimed verification is kept separate from the receiver's trust
assessment.** What a hop is worth to a reader is its *standing*, and there
are four:

| standing | what it means to the assessor |
| --- | --- |
| `claimed` | the hop asserts no verification: the writer's own claim |
| `asserted` | the hop asserts it was verified, and nothing the assessor trusts stands behind that. A forged stamp and a genuine stamp from an unauthorised relay both land here |
| `vouched` | verified through an explicit mechanism the assessor holds — an authorised relay on a trusted path, or evidence bound to the record |
| `established` | the assessor made this attestation itself |

The first two read the wire; the last two are the assessor's own
conclusion. This is RFC 8601's shape — an `Authentication-Results` field
records the checks its own boundary ran, and a result that arrived from
outside that boundary is a claim however it is labelled — and RFC 9440's
for a forwarded client certificate: an origin believes the `Client-Cert`
field only from a proxy it has authenticated. Divergences records the one
declared departure from RFC 9440 §2.4 this shape takes.

**[LOG-I13] The assessment starts from an anchor the assessor holds outside
the record.** The anchor MUST be the attestation the assessor established
itself, never anything read off the wire; with no anchor, no hop stands
above `asserted`. This is the trust anchor of RFC 5280 §6.1.1, delivered
out of band. A receiving service's anchor is the stamp it just appended; a
reader of a file no service wrote has none, and gets claims back, which is
exactly what a file can attest.

**[LOG-I14] From the anchor the path is walked downward.** A hop stands
`vouched` through an authorised relay only when the trusted hop above it
describes a party the assessor's policy authorises to attest the hops
beneath it. Being on the path is not being allowed to vouch: this is the
`cA` basic constraint of RFC 5280 §4.2.1.9, and certification path
validation, §6, is the walk. A genuine stamp carried by a relay the policy
does not name stands `asserted`, the same as a forgery — the receiver has
no basis to tell them apart, and says so rather than guessing.

**[LOG-I15] A hop bound to the record under a key the assessor holds
stands `vouched` with no path at all.** A binding that fails — a key the
assessor does not hold, or a record altered after the binding — stands
`asserted`. The binding covers the record's content and the chain up to
and including the bound hop, so a message, an attribute, a level, the
writer's claim or the bound subject changed afterwards breaks it. DKIM (RFC
6376) is the ancestor: evidence travelling with the message, verifiable
without trusting what carried it. Divergences records the declared
departure: the key is symmetric, HMAC-SHA256, because every platform
furnishes it and the assessor that holds the key is the trust root of its
own sink; a verifier can therefore forge what it verifies, and this is not
the mechanism for a reader that must not be trusted with that. The bound
bytes are the record as canonical JSON — keys sorted, no HTML escaping, no
trailing newline — which is RFC 8785's shape for the ASCII this module
writes and is **unproven against a second language** beyond it.

**[LOG-I7] The author is the attestation about the writer — hop 1 — and
only when the path from the anchor reaches it.** If only the immediate peer
is established, the author is unverified.

**[LOG-I8] The relay is the assessor's immediate peer: the hop it
established itself.** In a chain of one, author and relay are the same
answer, and the distinction is the whole reason the chain is kept.

**[LOG-I9] A record nothing has vouched for is unattributed, and says so.**
It is not read as the writer's claim promoted for want of anything better.

**[LOG-I10] A dispute compares claims about one subject.** The writer's
claim stands against the attestation about the writer, and a dispute is
resolved only when that attestation stands `vouched` or `established`;
both halves stay on the record. A process claiming to be the init system
while the kernel reports an ordinary account running an ordinary binary is
the interesting case. A relay carrying a different pid or host from the
writer is expected, not a dispute; and a contradiction the receiver has no
basis to trust is an unverified author, which is a different answer.

**[LOG-I11] An unestablished numeric identity is -1, never 0.** Zero keeps
its real meaning: a provider that could not determine a user id must not
be indistinguishable from one that determined the superuser.

**[LOG-I12] Where the platform's attester cannot answer, the record
records that it could not.** The line stays unattributed for a stated
reason rather than looking unattributed for an unknown one.

**What each attestation field carries.** `program` is the name a party
gives itself; the writer's `self` claim at hop 0 carries it. `exe` is the
executable path the attesting mechanism resolved for the party it
established: `so_peercred` reads it from the process table, `identity/<os>`
from the receiving boundary's program evidence. A receiving service's own
stamp leaves `program` empty, because a name is a claim and a stamp holds
only what its mechanism established. `user` is the account name or the
Windows SID; `uid` and `gid` stay -1 where the platform has no numeric
identity (`LOG-I11`). Readers compare the writer's `program` with the
stamp's `exe` as claim against evidence (`LOG-I10`) and never copy one into
the other.

#### What this design cannot refuse, stated

An authorised relay can forge anything beneath it, the way a certification
authority can. Authorisation is the policy's statement that it accepts
that. And a relay that is authorised by identity is authorised for the
hops it carries on a channel the receiver's kernel mediated; a record that
passes through a party the receiver does not trust, or rests in a store
between the stamp and the reader, needs the bound evidence of `LOG-I15`,
and this module has no transport yet on which that case arises.

### Pages

Naming this stops "the service will handle it" from standing in for a
design.

**[LOG-P1] A sink says what it actually enforces, and there are three
answers.**

| level | enforces | cannot do |
| --- | --- | --- |
| `none` | nothing. Attribution is by claim only | anything |
| `owner` | one file per account, enforced by the operating system | separate two applications run by one account |
| `peer` | the kernel's own answer about the connection: account, process, image | — |

A sink that cannot answer enforces nothing, which is the safe reading.

**[LOG-P2] `owner` is read from the mechanism the filesystem is enforcing,
not from the path convention.** A per-account path is advice; the mode on
disk is the mechanism, and a world-writable file separates nothing however
carefully its name suggests otherwise.

**[LOG-P3] Where a platform's access control cannot be read through this
interface, the sink reports `none`.** That is an admission rather than a
policy: a mode check on a platform that enforces with access-control lists
would be answering a question about a number that platform never set, and
anything deciding on separation must refuse rather than assume.

**[LOG-P4] A sink MUST report the separation the running platform can
actually enforce, and MUST NOT report the separation its mechanism
enforces elsewhere.** The runtime's logging service keeps this rule: it
binds every caller through [Program](https://openabstractions.github.io/reference.html#concept-identity.listener-program-need) proof on Windows, Linux and macOS
(`go/service/service.go`, `receiver.Write`). The standalone socket
provider (`go/service.go`, `Server`) does not: a sink that forwards to it
reports `peer` on every platform, while its peer-credential lookup answers
on Linux alone (`go/peer_linux.go`) and returns an error elsewhere, so on
those platforms every record it forwards is unattributed.

#### Retained-history pages

`logging.thrift` also defines `HistoryReader` and `HistoryObserver`, and
their bounded `Page` result. These services read through the selected
provider. The application supplies an opaque cursor and limits; the
provider selects history storage and retention. Reading requires
receiving-side Program evidence and the service account. Caller-supplied
record attestations remain claims. Authorization precedes provider access.

An empty cursor begins at the earliest retained record. The cursor word
`end` (`history_cursor_words`) begins at the current end of retained
history: a read returns a page with no records, `at_end` true and the
continuation at that end, and an observation fixes that end when the call
begins and returns only the records appended after it. A follower starts
there without paging through retained history. A provider never issues a
cursor word as a continuation, and a refusal returns the word unchanged. A
successful page preserves append order, returns at most 256 records and at
most 65,536 generated record bytes, and supplies its continuation.
Requested limits must be positive and within those caps. The encoded RPC
envelope may add framing/indentation overhead; the shared transport also
bounds the complete response. `at_end` describes the observed end for that
call. Repeating the continuation can reveal later appends.

A gap, invalid limit, unavailable history, corrupt record or oversized
first record returns its typed outcome with no records and the original
continuation. The caller explicitly chooses an empty cursor to restart
after a gap. A refusal never skips a record. The first record must fit the
requested byte budget to make progress.

The current FileSink provider reads its existing append-only history under
the same mutex as its writes. Cursors identify an open sink instance;
reopening invalidates them and retained records remain available through an
explicit fresh read. Detected file replacement or truncation returns
`gap`. External in-place rewriting is outside this provider's supported
append-only lifecycle: same-inode truncate-and-regrow between reads cannot
be reliably detected. The service-owned retained provider serializes rotation,
collection and reads with that writer mutex. Retained segments preserve cursor
positions; removal of unread history returns `gap`. Native providers supply
resolved byte/age policy through `OpenRetainedFileSink` and `SetRetention`.
There is no application-facing retention mutation API. Complete retention/replay
subscriptions remain implementation work.

The read response confirms retrieved history. The `Sink` interface's
`Write` remains one-way and confirms transport submission only. A stopped
or write-only provider cannot establish history availability. Runtime
registration advertises the reader only for a provider implementing it; a
logging host stopping withdraws both contracts. The Go sink, reader and
observer wait `client.DefaultTimeout`, two seconds, per call unless the
transport, `Observer.WithTimeout` or the call's context sets another
limit. A history policy answers within that wait: the installed runtime
bounds its rights decision to 500 ms and answers `policy_unavailable` past
it. Caller waiting cancellation uses shared IPC and does not cancel
accepted writes. Filesystem operations remain subject to OS completion; the
client waiting budget does not promise an interruptible filesystem call
inside the provider.

#### Scoped application history

`ApplicationHistoryReader` (`abstraction.logging/reader@2`) and
`ApplicationHistoryObserver` (`abstraction.logging/observer@2`) read the
account's service-owned retained OA application records. A request explicitly
selects `one_app` with a clean native subject program, or `all_apps` with no
program. All apps includes present and future programs whose records this
account's owning OA logger receives. Native operating-system sources retain
their separate source-reader API and exact source resources.

**[LOG-P5] Emitter identity.** The receiving logger derives account, subject program and captured
file identity from its Program-proven caller. Its private persisted emitter
stamp remains outside the public `Record` and its shrink ladder. Trusted
in-process runtime events use the same owned append boundary with the runtime's
captured self identity. Submitted identity chains, attributes, job names and
native producer claims supply no emitter authority. Generated decode/reencode
discards an incoming reserved stamp. Legacy unstamped rows and other-account
rows are excluded from @2; @1 retains its existing coarse record projection.
Shared/adopter-owned append files supply no authoritative app attribution and
do not advertise @2. Invalid private stamps in owned history return `corrupt`.

**[LOG-P6] Target checks.** One app checks the typed application-log target; all apps additionally
checks explicit collection admission. Rights owns selector matching and block
precedence. The logger supplies retained concrete emitter evidence to one quiet
final `CheckBatchFor` snapshot. Per-emitter refusals filter members; selection
or collection refusal ends the call. No policy callback runs with the provider
mutex held. Legacy whole-history checks use the shared compiled overlap helper:
an intersecting live typed block conservatively refuses the coarse call. Typed
app checks import applicable legacy history blocks. This overlap imports blocks
only; an old history permit grants no new app-log selector.

**[LOG-P7] Page bounds.** Each call copies at most 256 physical rows and 1 MiB from one observed
retained-history position before target filtering. Outgoing distinct emitter
checks stop before exceeding the shared 256-resource limit or the actual encoded
1 MiB `CheckBatchFor` frame, including the caller and selection admission.
Exclusion precedes the returned 1..256 record and 1..65536 encoded-record-byte
quotas. Empty pages may advance through excluded or blocked rows while
`at_end` remains false. An included first record exceeding the returned byte
budget yields `record_too_large`; excluded records consume no returned quota.
The owned physical scanner recognizes complete larger audit rows while they
fit the same 1 MiB scanned-work budget. A first physical row exceeding that
budget yields `record_too_large`, with no partial-row cursor, corruption or
end status; after earlier complete rows the oversized continuation remains
for the next call. Public reader and observer `max_bytes` stays 1..65536.
Oversized included audit rows may therefore be unavailable through these
published profiles. These profiles retain their audit-browsing availability
limit and native source-reader bounds.

**[LOG-P8] Sealed continuations.** Continuations are sealed to this host instance, caller, account,
selection, captured target and physical retained position. Resumption uses
captured target evidence and never reopens today's image to reinterpret old
rows. A changed selection, tampering, host restart or removed unread history
returns `gap` without records or cursor movement. The final batch snapshot is
the authority linearization point; later revocation affects the next batch.
The cursor grants no authority lease. `Observe` scans once, waits once only
at an empty physical end, then scans once more and checks a fresh final batch.
Wait remains within 0..30000 ms with bounded waiters and disconnect cancellation.
Selection admission is checked even for an empty or wholly excluded page.

**[LOG-P10] Retained usage.** `HistoryReader.Usage` uses the same native
admission and history-read authority as `Read`, rechecked before delivery.
An available answer reports retained physical record bytes and effective active
size/age budgets. An absent budget means known unlimited; a present limit's
value of zero means zero. `max_use` is bytes and `max_retention_ms` is
milliseconds, each using explicit optional scalar presence.
Unknown budgets return
`unavailable`. The optional oldest time is the first retained record's time in
append/Read order, absent for empty history. A malformed first retained row
returns `corrupt`; no usage values accompany a refusal. Providers bound the
first-record read and keep retention and cleanup ownership. An earlier reader@1
may return `unknown_method`; a caller reports usage unavailable. Existing
Read-only native providers keep their Read capability and return unavailable
for Usage.

**[LOG-P9] Records about the caller.** `ReadAboutCaller` returns the
records the receiving runtime wrote about the proven caller's own requests,
from `since` onward. The subject is the bound caller and never an argument.
A trusted in-process writer stamps such a record with the caller's program in
a private subject field beside the emitter stamp; the public `Record` is
unchanged, IPC and shared-file writers never set it, and earlier readers ignore
it. The runtime stamps its inference request records for a local proven caller
and the application command audit for the subject the call was decided for. A
subject relayed by a paired runtime is not a proven local caller and stays
unstamped. Admission is `abstraction.logging/history.read` on
`abstraction.logging/caller-log@1`, whose one target is the rule subject itself;
it grants no one-app or all-app application history, and an earlier
whole-history block covers it. Bounds, sealed cursors and `gap` follow LOG-P7
and LOG-P8; the cursor also binds `since` and the read mode. A read with an
empty cursor seeks to `since`: it skips retained segments last written before
it and positions inside the next segment by the instant the sink wrote each
row, starting one minute early, and still filters each row by record time.
Every row a retained sink appends, attributed or not, carries a private sink
instant; earlier readers ignore it. The sink writes it under its append lock,
nondecreasing in file order and continuing after a restart from the newest
instant written, so a writer's record times cannot move the position. A row
without the instant, written by an earlier runtime before the first stamped
row, is placed by its record time. Records the engine writes
under its
own program stay under application-log@1. Held by:
`go/service/caller_log_test.go`, `go/seek_test.go`, `abstraction-facade/go/grants/caller_log_test.go`,
`serve/runtime_inference_log_test.go` and `serve/application_v3_gateway_audit_test.go`.

`ApplicationLogPage` uses the closed `HistoryPageOutcome` vocabulary. Selection
or collection policy denial returns `not_permitted`; a decision that cannot be
obtained returns `unavailable`; invalid semantic bounds or selection returns
`invalid`. Business refusals contain no records, preserve the supplied cursor
and set `at_end` false. Caller identity and framing failures use the reply error
channel. `Page`, `PageOutcome`, `reader@1` and `observer@1` retain their published
wire shape and legacy refusal behavior.

Native Go clients and facade resolvers request exactly @2 and validate its
bounded empty-progress pages. Resolution never falls back to @1. Generated
bindings expose the same wire operations in the other supported languages.
Reads and final batch checks stay quiet. A caller explicitly requests consent
through `abstraction.asks/permission@1`; Panel can manage the same typed rules.
The selector's first-use metadata describes eligibility for that explicit
permission request. It promises no automatic popup on `Read` or `Observe`.
Legacy whole-history first-use remains suppressed.

The Go adoption path is to resolve `ResolveLogApplicationReader`, select a
program through `ApplicationLogTarget{Selection: ApplicationLogSelectionOneApp,
Program: program}`, and read its bounded page. When consent is needed, the
caller builds its explicit permission selection with action
`abstraction.logging/history.read`, resource profile
`abstraction.logging/application-log@1`, and selector `one` whose document
`ApplicationLogPermissionTarget(program)` encodes. The document contains only
the requested program; the trusted Rights owner captures its key. An `all`
selection contains no target document and explicitly includes the receiving
account's present and future OA emitters. After consent, the caller retries
the same exact @2 read. Panel can remove the rule or install a narrower block;
the next page checks the new shared policy revision.
The canonical application-log target document is bounded to 16 KiB, matching
the shared Rights target boundary; oversized requested or persisted targets
are refused by the owning profile.

#### Native operating-system sources

`logging.thrift` defines `SourceReader` at
`abstraction.logging/source-reader@1`. It reads one fixed, read-only native
source per call. A source ID is a stable identifier supplied by the runtime;
it is never a path or a caller-provided native query. `Sources` returns the
fixed source catalogue with outcome `catalog` and each source's current availability.
A catalogue-level refusal has no entries. An unavailable policy decision returns
the aggregate outcome `unavailable`. `available`
means that the native query facility is present under the granted source
scope. It does not promise complete capture. A later read may still receive
an operating-system refusal. The descriptor lists known retention,
native redaction, collection-policy and native-access limitations; missing
metadata is `visibility_unknown`. `SourceLimitation` is descriptive data; readers
preserve unknown limitation words for display. `SourceAvailability` controls
usability and rejects unknown words. An empty successful page establishes no
match in the source's visible retained data. It does not establish that no
event occurred.

The initial source IDs are `windows/application`, `windows/system`,
`windows/security`, `linux/system-journal`, `linux/user-journal` and
`macos/oslog`. They remain in the catalogue on every platform; sources without
a platform adapter report `unsupported`. A native query that finds an
operating-system access refusal reports `permission_denied`. A source
descriptor does not probe protected log content.

Each source read is authorized before the native adapter is probed or called.
It uses the existing exact action `abstraction.logging/history.read` with the
source-specific resource `source:<id>`. The separate resource prevents an
existing grant for `abstraction.logging/history` from granting OS-source
access. A denied catalogue entry has availability `permission_denied` and
causes no native access attempt. A policy-denied read or current-end operation
returns the typed outcome `not_permitted`; an unavailable rights decision uses
`unavailable`. A known native access denial returns `forbidden`. Native access
remains subject to the operating system's
own permissions and entitlements.

`SourceReader.Read` accepts only a fixed source ID and is bounded to 1..256
entries and 1..65,536 encoded `SourceRecord` bytes. A source adapter receives
the caller context and these limits and must stop work when the context ends.
An empty cursor starts at the oldest currently visible retained entry. A
bounded page may require repeated reads before reaching the current end;
recent-only tailing is a separate capability. The native source may refuse a
read even when its query facility is present.
Each cursor binds both the source ID and that reader instance. Reuse for a
different source, reader restart or lost retention returns `gap`, no records
and the original cursor; the caller explicitly restarts from an empty cursor.
`SourcePage` uses `HistoryPageOutcome`; invalid semantic bounds return `invalid`.
Refusals contain no records, preserve the supplied cursor and set `at_end` false.
Identity and framing failures retain the reply error channel. Pages preserve the native source's order.
The API promises neither cross-source ordering nor observation/subscription.

**[LOG-N1] A protected native page, once per approval.** On Windows, when
the runtime has a trusted elevated helper (abstraction-facade FAC-H2),
`windows/security` is a protected source. A `Read` the caller is permitted, by
the exact `history.read` on `source:windows/security` checked before and after
as above, whose native query the unelevated runtime is denied MUST NOT be
retried elevated by the runtime itself. It admits the system-access question
(abstraction-asks ASK-S1) for this program, source, opened native cursor and
bounds, and returns the typed outcome `os_authorization_required`. Once the
person answered `once`, and until the helper's page arrives within the claim
window, the same read refuses with `os_authorization_pending`. The helper runs
the same Windows adapter for the Security channel alone, with the same fixed
arguments and bounds; its `wevtutil.exe` child gets a fixed environment
(`SystemRoot` and `windir` from the Windows directory, `PATH` the system
directory alone, `LC_ALL=C`) and nothing of the helper's own. Its page is held in memory for at most 120 seconds,
validated, sanitized and sealed by the same rules as an adapter's page, and
returned once, to the same program, for the same source, cursor and bounds;
the caller's final source right is checked before it is returned. Nothing
protected is written to disk, and a restart drops a held page. An answer of
`refuse` or `never`, and a page the helper reports natively denied, refuse with
`forbidden`; an expired approval admits a new question at the next read.
Without a trusted helper (a per-user installation, a development layout,
macOS or Linux) the denial stays `forbidden`, and no question is admitted. All
protected waiting states and refusals preserve the requested cursor and return
no records; callers retry the same bounded operation after its existing approval
or helper state changes.
`StartAtEnd` is unprotected. Tests: `TestProtectedSourceRead`,
`TestProtectedSourceRefusals` (go/service).

`SourceReader.StartAtEnd` is an explicit cursor operation for callers that want
to begin after the currently observed end. It checks the same source-specific
right before native access and again before returning the sealed cursor. It
returns no records; a source with no safe native current-position operation
returns `unsupported`. An empty source still returns a valid sealed baseline,
so a later `Read` from that cursor can return entries appended afterward. When
there was no native entry to anchor the baseline, retention loss before the
first read may be undetectable. Restart and detectable retention loss return
`gap`. This operation does not watch, subscribe, or promise complete event
capture.

A `SourceRecord` reuses the common `Record` for normalized time, level,
message and string attributes. Its `Record.identity` is empty. Native provider
and process fields travel in the separate `ProducerClaim`, which has no
verification field and never represents the authenticated OA caller. No
native claim is promoted to OA identity. Adapters remove known secret-bearing
structured fields before returning an entry. Native message text may itself
contain sensitive content; this service makes no universal redaction claim,
and the caller needs the exact source right to receive it. A source read is a
direct bounded access; `Sink.Write` remains one-way and provides no checkpoint
acknowledgement for copying external records.

## Outcomes

| outcome | meaning | rule |
| --- | --- | --- |
| `page` | a successful read; records present, or an at-end empty page | — |
| `gap` | the cursor no longer matches retained history; the caller restarts from an empty cursor | — |
| `unavailable` | the provider has no history, or history could not be read | — |
| `invalid_request` | the requested limit is outside 1..256 records or 1..65536 bytes | — |
| `record_too_large` | the first retained record alone exceeds the requested byte budget | — |
| `corrupt` | a retained record failed to decode | — |
| `unsupported` | the provider does not implement observation | — |
| `caller_unavailable`, `identity_required`, `wrong_user` | reply error codes for caller identity failures | — |
| `policy_unavailable`, `forbidden` | legacy reader@1/observer@1 reply errors for an unavailable decision or policy refusal | — |
| `invalid`, `not_permitted`, `forbidden`, `unavailable` | new application/native history typed results for invalid semantic bounds, OA policy denial, native or boundary denial, and uncertainty | LOG-P6, LOG-N1 |
| `os_authorization_required`, `os_authorization_pending` | typed protected-source results while the existing approval or helper page is pending; read again with the same cursor and bounds | LOG-N1 |
| `runtime_unavailable`, or the resolver's own refusal | the facade's resolution error: no runtime installed or reachable, or the runtime offers no logging sink | LOG-S11 |

## Bounds

| what | bound | rule |
| --- | --- | --- |
| queue depth (async handler) | 1024 records by default | LOG-S12 |
| write timeout | 5 s by default | LOG-S12 |
| retry backoff | 100 ms, doubling to 5 s, ±20% jitter | LOG-S12 |
| `HistoryReader.Read` / `HistoryObserver.Observe` page | 1..256 records | — |
| page byte budget | 1..65536 encoded-record bytes | — |
| `HistoryObserver.Observe` wait | 0..30000 ms | — |
| Go sink/reader/observer default per-call wait | `client.DefaultTimeout`, 2 s | — |
| history policy decision budget | 500 ms, past which the runtime answers `policy_unavailable` | — |

## Divergences

- **LOG-I5.** RFC 9440 §2.4 sanitises an unauthenticated `Client-Cert`
  field away; a receiver here keeps the incoming field, at its standing,
  because `LOG-I4` and the reader's need for the evidence outrank the
  tidiness.
- **LOG-I15.** DKIM (RFC 6376) and RFC 8785 canonical JSON are the
  ancestors, but the binding key is symmetric, HMAC-SHA256, not
  asymmetric: every platform furnishes it, and the assessor holding the
  key is the trust root of its own sink, so a verifier can forge what it
  verifies. This is not the mechanism for a reader that must not be
  trusted with that.
- **LOG-P4.** A sink that forwards to the standalone socket provider
  reports `peer` on every platform, while that provider's peer-credential
  lookup answers on Linux alone. The runtime's logging service is outside
  this gap.

## Not built

- The native logger of `LOG-S15` in C++, Rust and JavaScript, planned for
  0.9.0 with the design's `ResolveLogOr`, `resolve_log_or` and
  `resolveLogOr`.
- An owned retention mutation API for retained history; complete
  retention/replay subscriptions remain implementation work.
- Standalone macOS Unix-socket calls lack Program proof and fail closed.
  The installed runtime uses XPC for Program-protected IPC calls.

## Next

- User, machine and run controls for `LOG-S14`'s retention keys, with
  retention-key locking, provenance and stamp, wait on the accepted config
  `reader@2`/`editor@2`/`observer@2` dependency.

## What a conformance run can reach

One driver exists, in Go, at `go/cmd/replay`, and the eight scenarios in
`testdata/scenarios/` are green against it on Windows and on Linux. Every
rule above is therefore **proven against one implementation and UNPROVEN
against any second**, which for a module whose claim is that several
languages agree on one record format is the gap that matters: a green
against one implementation proves that it agrees with itself.

**Reachable by a driver alone**, needing nothing but the machine it runs
on. Thirty-three rules, eight scenarios: the shape of one encoded record, a
reader refusing a schema, the delegation chain and each way it degrades, an
oversized record shrinking, a record whose retained metadata alone will not
fit, a cap too small for any record at all, a message of three-byte
characters cut to fit without splitting one, the identity chain along a
real two-hop path, a contradiction that must survive, and three attacks on
the chain — a forged earlier hop, an unauthorised relay, evidence altered
after it was bound — plus a forged binding, none of which is promoted, and
a record that arrives with no chain.

**Declared and reached by no scenario**, eight rules, each for a stated
reason:

| rule | why no scenario reaches it |
| --- | --- |
| `LOG-S7` | fanning out to several sinks needs a failing sink the vocabulary has no way to build, and inventing one for a rule this small buys a green nobody needed |
| `LOG-I3` | a naming convention for mechanisms. A driver that named parties instead would still answer with tokens, and no expectation could tell which it meant |
| `LOG-P2` | the mode is enforced on some platforms and not on others, so the same scenario would demand two different answers |
| `LOG-P3` | reachable only on a platform whose access control this interface cannot read, which is exactly the platform where the answer is a constant |
| `LOG-P4` | as above, and it is the rule the standalone socket provider does not keep |
| `LOG-S11` | the scenario vocabulary scripts the environment chain and has no facade resolver to leave absent. Each language's facade tests reach it instead: Go `TestAdoptedSinkWithNoRuntimeFailsWithTheResolutionError`, `TestResolveVerifiedWithNoRuntimeIsTheResolutionError` and the facade client's `TestNoRuntimeInstalledIsAResolutionError`; Python `test_adopted_logging_with_no_runtime_fails_and_substitutes_nothing`; C++ `facade_resolution_error`; Rust `adopted_sink_with_no_runtime_is_the_resolution_error`; JavaScript `absence.mjs` |
| `LOG-S12`, `LOG-S13` | the scenario vocabulary scripts one write at a time and has no clock, outage or queue. The shared fault `conformance/faults/sink-outage` reaches both in every language's tests instead: Go `TestSinkOutageFixture` and `TestBoundHandlerDeliversRecordsLoggedWhileTheRuntimeRestarts`; Python `test_sink_outage.py` and `test_handler.py`; C++ ctest `logging_sink_outage`; Rust `sink_outage_fixture`; JavaScript `test/logging.test.mjs` |

**Reached against a benched chain, and separately against a kernel.** The
scenarios hand the driver what a platform's attester answered, so the
arithmetic of the chain is judged on any machine. The same path — a
writer, a relay and a receiver on three unix sockets, the kernel stamping
both links — is run by this implementation's own tests on Linux, and
nowhere else: the standalone socket provider's peer-credential lookup
answers on one platform of three, and on the other two that provider
records that it could not attest and every record it accepts is
unattributed with that reason. The runtime's logging service binds callers
through Program proof on all three.

**Not reachable by any run, in any language but one.** Python, C++, Rust
and JavaScript have generated record codecs and logging clients, and
Python and C++ read the shared record fixtures in `testdata/records/`. No
second driver runs these scenarios. Until one does, a green proves that an
implementation agrees with itself.

**The shared runner's page.** The driver contract page carries this
module's section: its verdicts, the shape of its answers and the
operations the scenario files use.
