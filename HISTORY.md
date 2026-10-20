# abstraction.logging history

Prior art, provenance and what was tested for [CONTRACT.md](CONTRACT.md),
kept out of the contract itself per `research/vocabulary/DECISION.md` S7.

## Why the shape changed, 2026-09-23

CONTRACT.md declared its rule ids inline, `[LOG-R1]` bracketed at the end of
the sentence it named, named no `.thrift` it was the reading of, and carried
"What was tested where" and its superseded rules inline instead of on this
page.
`research/vocabulary/RENAME-PLAN.md` "logging (7 steps)" converted every id
to the head-of-paragraph `**[LOG-R1] Title.**` form, added the `Binds:`
line, the RFC 8174 boilerplate, the letter table and a Rules/Outcomes/
Bounds/Divergences/Not built shape, moved "What was tested where" and
"Superseded rules, not in force" here, and renamed three words: `rung`
(logging's fallback chain) to **fallback sink** (D22, `CONTRACT.md:88–108`,
`LOG-S2`, `S4`, `S5`, `S12`); `binding`, where it named one language's own
implementation of a record or store, to **implementation** (D28, sixteen
lines touching `LOG-R3`, `R4`, `R7`, `R8`, `S1`, `S8`, `S11`–`S13`, `I15`
and the conformance-reach prose — the connection-tie sense of `binding`,
D27, and the record's cryptographic binding under `LOG-I15` keep the word
unchanged); and `layer` to **module** (D6, fourteen lines). No described
behaviour changed in this pass, except the addition
`research/storage-classes/DECISION.md` §8 states for this page: `LOG-S14`,
held in CONTRACT.md's "Next" section because rotation and retention are not
yet built. README.md gained one sentence mapping `sink` to OpenTelemetry's
`exporter` (D37); `claimed`, `sink` and `hop` are kept words, unchanged.

## Prior art

RFC 5424 (syslog) is `slog`'s and this page's level scale's ancestor.
OpenTelemetry's log data model, Serilog, Flume and Kafka Connect name the
same role `sink` (D37; a download's own sink is *destination*, D36). RFC
8601 `Authentication-Results`, RFC 9440 forwarded client certificates, RFC
5280 certification path validation, DKIM (RFC 6376) and RFC 8785 canonical
JSON are the identity chain's ancestors, cited inline in CONTRACT.md at the
rule each informs (`LOG-I5`, `LOG-I13`, `LOG-I14`, `LOG-I15`).

## Superseded rules, not in force

Kept for the reason each was wrong, not deleted.

- **LOG-I5 draft, struck 2026-09-09.** Stripping a verified attestation on
  send and on accept deleted a forgery and the genuine article alike.
- **LOG-I7 draft, struck 2026-09-09.** The earliest hop asserting
  verification is a claim about the author, not the author; reading it as
  the author is how a forged earlier hop gets promoted.
- **LOG-I8 draft, struck 2026-09-09.** Last on the wire is where a forger
  writes; the relay is the hop the receiver establishes itself.

## Tested

CONTRACT.md's "What a conformance run can reach" states which of today's
rules the eight scenarios and each language's own tests cover, and which
do not run at all yet. This table carries what was tested where.

| | ran | did not run |
| --- | --- | --- |
| the scenarios | against this implementation's driver on a Windows workstation and on Linux under WSL2, every cited rule shown to fail when broken in the implementation | against any second language, on any platform |
| the record format | Go's unit tests; Go, Python and C++ reading the shared fixtures `attestation-fields.jsonl` and `unclaimed-hop0.jsonl` | Rust and JavaScript on the fixtures; any second language under the scenarios |
| the identity chain | the walk, the binding and each attack in unit tests on both platforms; the real two-hop socket path on Linux | the two-hop path on Windows or macOS, where the socket sink's kernel lookup is not written |
| peer credentials | the socket `Server`: one kernel's own unit tests, in one language. The runtime logging service (`go/service`): its `identity/<os>` stamp from the receiving boundary's evidence, in unit tests on Windows, and in real ComfyUI runs on Windows and a real Ollama run on Linux | the socket `Server` on every other platform, where the lookup returns an error; the runtime service's stamp on macOS |
| the service | the socket `Server` as a real listener under the driver for the tier scenario; the runtime logging service in its unit tests and hosted by the installed runtime (`openabstractions`) with a file history provider | the socket `Server` as a daemon. Nothing ships one |
| absence (`LOG-S11`) | a resolved service sink with no runtime in Go, Python, C++, Rust and JavaScript tests, each failing with the resolution error | an adopter's end-to-end run with no runtime installed |
| a lost binding (`LOG-S12`, `LOG-S13`) | the sink-outage fixture against a fake service sink in Go, Python, C++ (MSVC), Rust and JavaScript on Windows, with equal counts and gap records that sum to the dropped count, including a drop while the gap record is delivered; in Go, a drop during the drain carried into a second failure's gap record; in Go, a real resolver and logging service stopped and restarted under a bound handler | a supervised runtime killed under an installed adopter; Linux and macOS |
| retained history | `go test ./...` in `logging/go` exercises retained records, Unicode, pagination, append-at-end, invalid limits, oversized/corrupt data, reopen gaps and same-account service authorization; `conformance/clients/facade/run.py --run` builds an installed C++ consumer that resolves the history service and checks record content/provenance, continuation, gap and byte-limit refusal through Go services | a second-language driver against `HistoryReader`/`HistoryObserver`'s own scenarios |

## 0.8.0 contract truth pass (W0-04), 2026-10-21

Each changed or reviewed statement with its class: an outdated rule is text the code has passed, a code defect is code or IDL that breaks a rule and becomes a TODO row, an accepted divergence is recorded under Divergences.

| statement | class | action |
| --- | --- | --- |
| LOG-S14 under Next | outdated rule | moved to Rules under Sink; the config @2 controls stay under Next |
| LOG-P5..P8 declared without brackets | outdated rule | bracketed heads, so `[LOG-P5]`..`[LOG-P8]` resolve |
| Letter `N` missing from the letter table | outdated rule | added |
| LOG-P4 and the conformance section said the service cannot attest | outdated rule | scoped to the standalone socket provider; the runtime service binds on three platforms |
| README: account-wide history gate | outdated rule | reader@2 and observer@2 are served |

## reader@2 ReadAboutCaller (A-F19), 2026-10-31

LOG-P9 is new. `ApplicationHistoryReader` gains `ReadAboutCaller`, an additive
reader@2 method under CONV-V1, with the `caller-log@1` Rights profile and a
private subject stamp beside the emitter stamp. Earlier runtimes ignore that
stamp when they read the same history. Existing methods, structs and grants are
unchanged. A read with an empty cursor seeks to `since`.
Every row a retained sink appends carries a private sink instant; earlier
readers ignore it.

Back to [CONTRACT.md](CONTRACT.md).
