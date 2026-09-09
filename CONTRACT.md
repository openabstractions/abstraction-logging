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

- **It mints no identity.** No accounts, no tokens, no registration, no
  secrets. Every useful identity provider is one the operating system already
  operates, and an identity this layer invented would have to be defended by
  this layer and would be worth exactly as much as a self-declared field.
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
writer** [LOG-I1]. Hop 0 is the process that wrote the record.

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

**A verified attestation a party attached to its own record does not survive
being sent** [LOG-I5]. It is stripped before the record leaves the process and
again by the party that accepts it, because only the party that ran a check may
record that the check passed. Without that, "verified" would mean "the sender
wrote true", which is worse than useless: it looks like assurance.

**A service appends what it could establish and overwrites nothing** [LOG-I6].
Its attestation is a new hop, and the hops in front of it are still there.

**The earliest verified attestation is the author** — whoever originally spoke
[LOG-I7]. **The last verified attestation is the relay** — whoever most recently
handed the record on [LOG-I8]. In a chain of one they are the same answer, and
the distinction is the whole reason the chain is kept.

**A record nothing has vouched for is unattributed, and says so** [LOG-I9]. It
is not read as the writer's claim promoted for want of anything better.

**A verified attestation that contradicts the claim makes the record disputed,
and both halves stay on it** [LOG-I10]. A process claiming to be the init system
while the kernel reports an ordinary account running an ordinary binary is the
interesting case, and it is only expressible because nothing was discarded.

**An unestablished numeric identity is -1, never 0** [LOG-I11]. Zero keeps its
real meaning: a provider that could not determine a user id must not be
indistinguishable from one that determined the superuser.

**Where the platform's attester cannot answer, the record records that it could
not** [LOG-I12]. The line stays unattributed for a stated reason rather than
looking unattributed for an unknown one.

### The one question this page does not settle

A relay chain has two verified hops, and the rules above say what each of them
means. What no rule here says is how the party in the middle tells a stamp its
predecessor made from a forgery the original writer attached. On the wire they
are the same bytes.

Two answers, and they are a real choice rather than a detail:

- **Strip every verified attestation on accept.** Nothing forged can survive,
  and neither can anything genuine: a chain can then never hold more than one
  verified hop, the earliest and the last are always the same attestation, and
  the relay argument that justifies keeping a chain at all is unreachable.
- **Strip only what the sending party asserted about itself.** A relay chain
  works and reads as it was designed to, and a writer that attaches an
  attestation attributed to a hop it did not occupy is believed.

Neither is chosen here. `LOG-I5` states the half both answers deliver — an
attestation a party made about itself does not survive its own send — and the
scenario that reaches the disputed half asserts nothing about it and says so.

**The implementation shipped today takes the first answer**, and strips every
verified attestation from every record it accepts. So the two-hop chain
`LOG-I7` and `LOG-I8` are specified against cannot be produced by it, and its
own two accessors — the earliest verified attestation and the last — cannot
return different answers on any record it has ever written. A reader should
know that before believing either of them means what it says.

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

A driver for this layer does not exist, in any language. Every rule below is
**UNPROVEN against every implementation**: the scenarios have been shown to fail
when the rule each cites is deliberately broken, which is a fact about the
scenarios and not about any implementation.

**Reachable by a driver alone**, needing nothing but the machine it runs on.
Twenty-six rules, six scenarios in `testdata/scenarios/`: the shape of one
encoded record, a reader refusing a schema, the delegation chain and each way it
degrades, an oversized record shrinking, the identity chain from the writer's
claim through a stripped forgery to two relays, and a contradiction that must
survive.

**Declared and reached by no scenario**, five rules, each for a stated reason:

| rule | why no scenario reaches it |
| --- | --- |
| `LOG-S7` | fanning out to several sinks needs a failing sink the vocabulary has no way to build, and inventing one for a rule this small buys a green nobody needed |
| `LOG-I3` | a naming convention for mechanisms. A driver that named parties instead would still answer with tokens, and no expectation could tell which it meant |
| `LOG-P2` | the mode is enforced on some platforms and not on others, so the same scenario would demand two different answers |
| `LOG-P3` | reachable only on a platform whose access control this interface cannot read, which is exactly the platform where the answer is a constant |
| `LOG-P4` | as above, and it is the rule this layer does not keep |

**Not reachable by any scenario, and the gap that matters most.** The rung of
the chain that makes this layer worth having — a service that asks the kernel
who sent a record — is present as a library and is run by nothing. There is no
service to point a driver at, the peer-credential lookup behind it answers on
one platform of three, and the identity rules above are therefore proved against
a benched chain rather than against a kernel. `LOG-I5`, `LOG-I6`, `LOG-I7`,
`LOG-I8` and `LOG-I12` are judged on what an implementation does to a chain it
is handed, and are **UNPROVEN against a real connection** on every platform.

**Not reachable by any run, in any language but one.** This layer's claim is
that several languages agree on one record format. One binding exists. Until a
second one runs these scenarios, a green proves that an implementation agrees
with itself, which is the thing a conformance suite exists to stop counting as
evidence.

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
| the scenarios | against a transcript replaying this page's own answers, on a Windows workstation, every cited rule shown to fail when broken | against any implementation, in any language, on any platform |
| the record format | one language's own unit tests | any second language — the second binding is an empty directory |
| peer credentials | one kernel's own unit tests, in one language | every other platform; the lookup returns an error there and no scenario reaches it |
| the service | its unit tests, as a library | as a running service. Nothing ships one, and nothing this project runs writes where a reader could see |
