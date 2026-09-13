# abstraction-logging

Applications send structured events through a resolved local service. The service
owns the sink and retained history. Generated protocols and the shared IPC
transport support independently adopted clients; applications do not open the
service's log files.

## Application clients

Go callers can install `logging.Default("my-app")` as a `slog.Handler`. It resolves
and retains one verified installed-runtime binding. Missing installation or
service refusal is returned by `Handler.Handle`; standard `slog` calls discard
handler errors, so applications needing explicit delivery diagnostics should use
the resolved client's error-returning methods.

Use the [facade](https://github.com/openabstractions/abstraction-facade) for resolved
Go, C++, Python, Rust or JavaScript client entrypoints and their package/trust
requirements. [C++ setup](cpp/README.md) describes its installed package. Select
coordinated revisions containing the required APIs; source support does not mean
every package is published or every platform qualified. Default verified local
installation selection is implemented for Go/C++/Python on Windows and supported
Linux. Rust/JavaScript require explicit independent server-trust configuration.
macOS lacks the required local Program proof.

Sink completion confirms local frame submission. It carries no durable logging
receipt. Reader and observer contracts expose bounded history, explicit gap and
refusal outcomes. Cancellation ends the current wait. See [CONTRACT.md](CONTRACT.md)
and [logging.thrift](logging.thrift) for the service behavior and limits.

## Explicit provider compatibility

The Go file sinks, raw-record stream and `Auto`/`LegacyAuto` remain deliberately
selected provider APIs. `LegacyDefault` retains the earlier stderr/file behavior.
Keep these choices explicit when embedding a provider; `Default` never selects
a local file or stderr fallback after a service failure. Existing configured
records are read by the service-owned history provider.

The Go host supplies the provider; C++/Python/Rust/JavaScript client availability
is distinct from a native provider implementation in those languages. Consult
[coverage](https://github.com/openabstractions/abstractions)
and release-specific evidence for tested scopes and platforms.

[Apache-2.0](LICENSE)
