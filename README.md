# abstraction-logging

*For applications that write logs, and operators who read them.*

Send structured application events to one local service and inspect bounded
history later. The service chooses the destination, attributes the caller and
keeps retained [records](https://openabstractions.github.io/reference.html#concept-log.record). Applications never need the service's log-file path.
A service in place of each program's own local logger gives one history for
the whole machine, with each record attributed to the program that wrote it by
what the kernel reports about the caller (see
[Reading history](#reading-history-how-a-record-is-attributed)).

**What's new here:** One log for the whole machine: each record is attributed
to the program that wrote it from what the kernel reports, and authorized tools
read or follow it.

Install the runtime first: https://openabstractions.org/adopt.html

Go, C++, Python, Rust and JavaScript clients use the same event and history
contracts through the shared local transport.

The [permission journey](https://github.com/openabstractions/abstractions/blob/main/examples/rights-playground/README.md)
explains the common operation/resource model and its accepted one/all breadth
rules. Application-scoped history is served: `abstraction.logging/reader@2`
and `observer@2` read one application's records, or every application's,
under rights' typed application-log target (CONTRACT.md LOG-P5 to LOG-P8).

## Send a [record](https://openabstractions.github.io/reference.html#concept-log.record)

Go callers install [`logging.Default("my-app")`](https://openabstractions.github.io/reference.html#concept-logging.default-handler) as the `slog` handler.
It queues each record at once and delivers it in the background through one
verified installed-runtime binding. `defer h.Close` delivers the queued
records before `main` returns, waiting up to 5 s without a deadline.

```go
package main

import (
    "context"
    "log/slog"

    logging "github.com/openabstractions/abstraction-logging/go"
)

func main() {
    h := logging.Default("my-app")
    defer h.Close(context.Background())
    slog.SetDefault(slog.New(h))
    slog.Info("worker started", "component", "indexer")
}
```

On a machine without OA, the records go nowhere unless the program installs
a native logger beside the facade (CONTRACT.md LOG-S15). Go passes any
`slog.Handler`, and Python any `logging.Handler`:

```go
h := logging.DefaultWithNative("my-app", slog.NewTextHandler(os.Stderr, nil))
```

```python
from abstraction.logging.handler import resolved_handler

logging.getLogger().addHandler(resolved_handler(program="my-app", native=logging.StreamHandler()))
```

While the machine offers no logging service, records go to that handler.
OA takes over when it is installed, without a restart. C++, Rust and
JavaScript get the native logger in 0.9.0.

Python callers attach `ServiceHandler` from `abstraction.logging.handler` to
the standard `logging` module.

```python
import logging
from abstraction.facade.client import Machine
from abstraction.logging.handler import ServiceHandler

handler = ServiceHandler(Machine().resolve_log(), program="my-app")
logging.getLogger().addHandler(handler)
logging.getLogger("my-app").warning("disk %s nearly full", "D:")
print(handler.counts())
```

Every language resolves the same contract, `abstraction.logging/sink@1`:

| language | resolve | handler that survives an outage |
|---|---|---|
| Go | `logging.Default(program)`, or `logging.Resolve(ctx, program)` for the resolution error explicitly | the resolved `Handler`, an `AsyncSink` in front |
| Python | `abstraction.logging.handler.resolved_handler(program=...)` | `ServiceHandler` |
| C++ | `facade::Machine{}.resolve_log()` — see [C++ setup](cpp/README.md) | `logging::make_async_handler(logger, resolve)` |
| Rust | `ResolvedSink::resolve(machine, ..)` — see [Rust setup](rust/README.md) | `AsyncWriter` |
| JavaScript | `ResolvedSink.resolve(machine, SinkClient)` from `@openabstractions/facade/logging` | `ResolvedSink` |

OpenTelemetry calls this same role an *exporter*; this project names it
*sink*, following Serilog, Flume and Kafka Connect
(`research/vocabulary/DECISION.md` D37).

Standard `slog` and Python's `logging` discard a handler's write errors; an
application that needs explicit delivery diagnostics calls the resolved
client's error-returning methods directly.

[Sink](https://openabstractions.github.io/reference.html#concept-log.sink) completion confirms local frame submission and carries no durable
logging receipt. Reader and observer contracts expose bounded history,
explicit gap and refusal outcomes. See [CONTRACT.md](CONTRACT.md) and
[logging.thrift](logging.thrift) for the service behavior and limits.

## When the service is absent or disappears

When the service is absent, resolution and every write fail with the
facade's resolution error, and the record goes to no other sink (`LOG-S11`).
When it disappears after binding, the handler holds up to 1024 queued
records, retries delivery with backoff, and delivers one gap record on
recovery naming what was dropped (`LOG-S12`, `LOG-S13`).
`conformance/faults/sink-outage` is the fixture every language's tests run
against this behavior.

## Explicit provider compatibility

The Go file sinks, raw-record stream and `LegacyAuto` remain deliberately
selected provider APIs. `LegacyDefault` retains the earlier stderr/file
behavior. `Auto` was removed; applications use `Default`, and deliberate
adopters of the environment/file provider call `LegacyAuto`. Keep these
choices explicit when embedding a provider; `Default` never selects a local
file or stderr fallback after a service failure. Existing configured records
are read by the service-owned history provider.

The Go host supplies the provider; C++/Python/Rust/JavaScript client
availability is distinct from a native provider implementation in those
languages. Consult [coverage](https://openabstractions.org/coverage.html) and
release-specific evidence for tested scopes and platforms.

## Service-owned history retention

The Go runtime and standalone logging service select a single-writer retained
file provider. Its default `logs_max_use` is 10% of the containing filesystem,
capped at 4 GiB; `logs_max_retention` is unset. The active file and completed
JSONL segments together stay within that encoded-byte limit. Filesystem
metadata and allocation rounding are outside this byte count.

The provider rotates monthly and by size, collects expired segments by their
newest write time, and removes the oldest segments to meet the size limit.
Open, write and history read enforce limits; the host also collects daily.
Readers continue through retained rotations. Removed unread history returns
the existing `gap` outcome with the original cursor. Reopening requires a fresh
cursor. Retention failures reach the host's error callback; a single encoded
record larger than the byte limit is refused. Log submission remains one-way.

Native providers select resolved values through
`OpenRetainedFileSink(path, LogsRetention{MaxUse, MaxRetention})` and
`FileSink.SetRetention`. `OpenFileSink` retains its shared append-only behavior.
The user, machine and run retention controls, machine locking, provenance and
stamp remain accepted config `@2` work. Current config `@1` has no retention
settings. Retention touches only this provider's completed log segments.

Go service owners needing exact audit retention can use
`FileSink.EncodeAttributedExact(record, emitter, physicalCap)` for the complete
row size check and `WriteAttributedExact` for append, or bind the same cap with
`WithExactEmitter`. These additive helpers preserve every value and private
emitter evidence, including the newline in the cap. They require the existing
owned single-writer retained store. The service derives its finite cap from
its admitted inputs and metadata; a size fit does not reserve disk space.
Ordinary `MaxLine`, attributed writes and shrink behavior remain unchanged.
Current history profiles keep their 65536-byte output and 1 MiB scanned-work
bounds; an oversized first row returns `record_too_large`. Exact retention
does not promise that every audit row can be retrieved through those profiles.

A small-limit fixture writes 200 records in an automatically removed temporary
directory, checks the retained byte bound, and demonstrates an old cursor gap:

```sh
go -C go test . -run TestRetentionSmallLimit -v -count=1
```

Run from the logging module checkout root. In the private aggregate checkout,
replace `go` after `-C` with `openabstractions-flat/abstraction-logging/go`.

## Current limits

- A successful sink call confirms bounded local submission. It is not a
  durable persistence receipt.
- Standard `slog` discards handler errors. Use the resolved client directly
  when the application must react to delivery failure.
- The installed macOS runtime binds protected IPC calls to [Program](https://openabstractions.github.io/reference.html#concept-identity.listener-program-need) proof over
  XPC. Standalone Unix-socket calls lack that proof and fail closed. The retained
  [0.3.0 installed check](https://github.com/openabstractions/abstractions/blob/main/research/release-0.3.0/macos-installed/logs/installed_check.log)
  resolved the logging XPC endpoint; its
  [receipt](https://github.com/openabstractions/abstractions/blob/main/research/release-0.3.0/macos-installed/receipt.json)
  identifies the tested package. It does not qualify the current revision's
  logging behavior.
- User, machine and run retention controls remain config `@2` work.
- Source support does not establish that each language package has been
  published or independently qualified at the selected revision.

## Reading history: how a record is attributed

A shared sink is useful only when lines can be attributed, and attribution is
useful only when it cannot be forged. Identity is an ordered chain: hop 0 is
what the writer says about itself, and every service the record passes
through appends what the kernel told it about its immediate peer, and nothing
is removed. Each hop stands `claimed`, `asserted`, `vouched` or `established`;
a reader assesses the chain from an anchor it holds itself and a policy
naming which relays it trusts. Logging's attestation standing `claimed`
differs from identity's proof rung `claimed`; see
[abstraction-identity](https://github.com/openabstractions/abstraction-identity).
[CONTRACT.md](CONTRACT.md) states the rules.

[Apache-2.0](LICENSE)

<!-- BEGIN GENERATED PERMISSION DEFINITIONS -->

## Permission definitions

Generated from the existing compiled catalogue. [RIGHTS.json](RIGHTS.json) contains the complete action/profile records, disclosure, lifecycle, first-use behavior, selector support and finite bundles.

This reference records compiled receiving support; RIGHTS.json names the compilation context. A connected runtime advertises current availability and selector support. These definitions supply no grant.

| Action | Resource profile | Owning operations | First-use targets | Compiled receiver |
|---|---|---|---|---|
| `abstraction.logging/history.read` | `abstraction.logging/application-log@1` | `abstraction.logging/reader@2#Read`, `abstraction.logging/observer@2#Observe` | One, All | served in compiled context |
| `abstraction.logging/history.read` | `abstraction.logging/caller-log@1` | `abstraction.logging/reader@2#ReadAboutCaller` | One | served in compiled context |
| `abstraction.logging/history.read` | `abstraction.rights/legacy-exact-string@1` | `abstraction.logging/reader@1#Read`, `abstraction.logging/reader@1#Usage`, `abstraction.logging/observer@1#Observe`, `abstraction.logging/source-reader@1#Sources`, `abstraction.logging/source-reader@1#Read`, `abstraction.logging/source-reader@1#StartAtEnd` | `none` | served in compiled context |

<!-- END GENERATED PERMISSION DEFINITIONS -->
