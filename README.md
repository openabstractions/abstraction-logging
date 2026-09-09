# abstraction-logging

A log record leaves the process that wrote it, in one encoding every language
reads, carrying who wrote it and how strongly that was proven.

## The problem

`log/slog` in Go and `logging` in Python already give an application an
interface with swappable handlers, and both are in the standard library. What
neither can do is leave the process: `slog.Handler` and `logging.Handler` are
in-process interfaces, so a question like "what was the file server doing with
my transfer while this laptop was asleep" cannot be answered from a program's
own log. This layer supplies the two missing things — a record format with a
fixed cross-language encoding, and sinks that write somewhere several processes
on several machines can share. Attribution matters once a sink is shared, so a
record carries an ordered identity chain: what the writer claimed about itself,
and separately what a receiving service could establish from the kernel.

## Words

| word | meaning |
|---|---|
| **record** | one JSON line: `schema`, `time`, `level`, `msg`, `identity`, `job`, `attrs` |
| **sink** | a destination a record is handed to: a shared file, a local socket service, nothing |
| **attestation** | who wrote a line and which mechanism says so: `BySelf` for the writer's own claim, `ByPeerCred` for one the kernel supplied |
| **identity chain** | the ordered attestations a record carries; a service strips any verified hop a client attached and appends what the kernel reported |
| **separation** | what a sink enforces between writers: `SeparationNone`, `SeparationOwner`, `SeparationPeer` |

No rule on this page carries a tag, and no conformance scenario cites this
layer.

## Obtain

- **Go.** `go get github.com/openabstractions/abstraction-logging/go`. The
  module path ends in `/go`; the package is `logging`, so import it with an
  explicit alias. The newest tag is `go/v0.1.0`; `@main` is the tree as it
  stands.
- **Python, C++.** None.

## Example

```go
package main

import (
	"log/slog"
	"os"
	"path/filepath"

	logging "github.com/openabstractions/abstraction-logging/go"
)

func main() {
	dir, err := os.MkdirTemp("", "logging-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// Auto picks a sink from the environment: a socket service if
	// ABSTRACTION_LOG_SERVICE is set, else a file if ABSTRACTION_LOG is set,
	// else a working no-op.
	path := filepath.Join(dir, "abstraction.jsonl")
	os.Setenv(logging.EnvSink, path)

	// One line of setup. Every slog call in this program, and in every library
	// it imports, now lands in that file.
	slog.SetDefault(slog.New(logging.NewHandler(logging.Auto("example"),
		&logging.Options{Program: "example"})))

	slog.Info("fetching weights", "repo", "org/model", "bytes", 379400000)

	written, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	os.Stdout.Write(written)
}
```

One JSON object is written, on one line, carrying `schema`, `time`, `level`,
`msg`, an `identity` chain and `attrs`. `bytes` survives as the string
`379400000` rather than a float: attribute values are strings, because the
format crosses language boundaries and JSON numbers do not round-trip an int64
through every reader.

## API overview

**Records.** `Record` holds `Schema`, `Time`, `Level`, `Msg`, `Identity`, `Job`
and `Attrs map[string]string`. `Encode()` writes one newline-terminated JSON
line; `DecodeRecord(line)` reads one and refuses a `Schema` other than the
current constant, `1`. `Timestamp` always marshals as UTC with exactly six
fractional digits and a trailing `Z`, accepts anything RFC 3339 on read, and is
built by `At(time.Time)`. `Level` is `LevelDebug`, `LevelInfo`, `LevelWarn`,
`LevelError`, numbered to match `slog.Level`.

**Sinks.** `Sink` is one method, `Write(Record) error`. `OpenFileSink(path)`
appends to a file several processes may hold open at once, one record per
`Write` call so concurrent appends interleave whole lines; a record longer than
`MaxLine` (default `DefaultMaxLine`, 3800) has its message truncated and marked
rather than split. `DiscardSink{}` accepts and drops; `MultiSink` is a slice
written in turn. `FromEnv()` returns a file sink from `ABSTRACTION_LOG`
(`EnvSink`), or a `DiscardSink` when it is unset or cannot be opened.
`PerUser(root, user)` gives the conventional per-user path. `SeparationOf(sink)`
reports what a sink enforces; `FileSink.Separation()` reads the mode on disk,
and reports `SeparationNone` on Windows, where `os.FileMode` cannot see an ACL.

**slog bridge.** `NewHandler(sink, *Options) *Handler` implements
`slog.Handler`. `Options` carries `Program`, `Level`, `Job` and `Claim`. Groups
flatten into dotted attribute keys.

**Identity.** `Claim(program)` builds a `BySelf` attestation. `Identity` is the
ordered chain, with `Claimed()`, `Verified()`, `Relay()`, `By(mechanism)` and
`Disputed()`; `Record.Trusted()` reports whether any hop was independently
verified. `PeerOf(net.Conn)` returns the kernel's answer via `SO_PEERCRED` on
Linux and an error elsewhere.

**Service.** `NewServiceSink(addr, fallback)` sends to a listener and falls back
when nothing is there. `Server` (`Listen`, `Serve`, `Addr`, `Close`) accepts
connections, strips any verified attestation a client attached, appends what the
kernel reported, and passes the record to `Server.Out`. `Auto(program)` returns
a service sink when `ABSTRACTION_LOG_SERVICE` (`EnvService`) is set, otherwise
whatever `FromEnv` gives.

## Today

Experimental, version 0.1.0. **Go only**, no adopter outside this organisation.

- **No Python implementation.** The record format was designed so one could be
  thin, but it does not exist, and neither does the cross-language conformance
  test that would make this a shared format rather than one program's file.
- **No verified peer identity except on Linux.** `PeerOf` returns an error on
  Windows and macOS; Windows would need a named pipe and the token API.
- **The service is a library, not a program.** `Server` works; nothing here runs
  it.
- **No rotation or retention.** A file sink grows until something else removes
  it.
- `Auto` ignores its `program` argument; the claim comes from `Options.Program`.

## Conformance

None across languages; there is one implementation. 8 tests run on any
platform, plus 4 behind `//go:build linux` that need a real kernel for
`SO_PEERCRED`: `cd go && go test ./...`.

## Where it sits

Below: [abstraction-job](https://github.com/openabstractions/abstraction-job)
(a record may name the job it was written under). Above:
[abstraction-asks](https://github.com/openabstractions/abstraction-asks),
[abstraction-rights](https://github.com/openabstractions/abstraction-rights)
and [abstraction-facade](https://github.com/openabstractions/abstraction-facade)
import it.

One layer of [openabstractions](https://github.com/openabstractions/abstractions).
Every layer names one thing local tools rebuild on their own; the name means the
same in each language that implements it, and the conformance scenarios are what
hold an implementation to it.

## Requirements

Go 1.26 or newer. No dependencies outside the standard library. Builds on
Windows, Linux and macOS; the socket service and peer credentials are Linux-only
in practice.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
