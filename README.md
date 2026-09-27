# abstraction-logging

Install the runtime first: https://openabstractions.org/adopt.html

Send structured application events to one local service and inspect bounded
history later. The service chooses the destination, attributes the caller and
keeps retained records. Applications never need the service's log-file path.

Go, C++, Python, Rust and JavaScript clients use the same event and history
contracts through the shared local transport.

## Send a record

Go callers can install `logging.Default("my-app")` as a `slog.Handler`. It
resolves and retains one verified installed-runtime binding.

```go
package main

import (
    "log/slog"

    logging "github.com/openabstractions/abstraction-logging/go"
)

func main() {
    slog.SetDefault(slog.New(logging.Default("my-app")))
    slog.Info("worker started", "component", "indexer")
}
```

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

Sink completion confirms local frame submission and carries no durable
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

## Current limits

- A successful sink call confirms bounded local submission. It is not a
  durable persistence receipt.
- Standard `slog` discards handler errors. Use the resolved client directly
  when the application must react to delivery failure.
- Current macOS source binds protected IPC calls to Program proof over XPC.
  The Unix-socket path fails closed, and the published 0.2.0 package predates
  XPC support.
- No rotation or retention is implemented; the service keeps what its
  bounded history retains and no more.
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
