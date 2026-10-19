# Logging from C++

*For C++ applications.*

Send structured events to the machine's logging host. The C++ client contains no
file sink, store, listener or local fallback. `Logger::Log` supplies schema and
timestamp; applications supply the event.

**Development source:** this path is not a claim that a published package or an
OS-installed service is available. C++ clients use the shared IPC runtime; the
host is Go.

## Through the facade

Install the runtime first: https://openabstractions.org/adopt.html

With development packages installed under `CMAKE_PREFIX_PATH`:

```cmake
find_package(abstraction_facade CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE abstraction::facade_client)
```

```cpp
#include <abstraction/facade/client.hpp>

auto events = abstraction::facade::Discover().Log();
events.Log(0, "worker started", {{"component", "worker"}});
```

`my_app` is your existing CMake target. Discovery is lazy: the call to `Log`
reports connection or encoding failure by exception. Handle it at your
application's error boundary. There is no silent file fallback.

For a dependency on logging alone, use
`find_package(abstraction_logging CONFIG REQUIRED)` and link
`abstraction::logging_client`; include `<abstraction/logging/client.hpp>` and
construct `abstraction::logging::Logger`. Both routes use the same generated
`SinkClient` protocol and shared `abstraction::ipc` transport.

## Keep logging while the runtime restarts

`Logger::write` is synchronous and reports each failure by exception. An
application that logs from its working threads keeps an `AsyncHandler`
(`<abstraction/logging/async_handler.hpp>`, contract `LOG-S12` and `LOG-S13`):

```cpp
#include <abstraction/facade/client.hpp>
#include <abstraction/logging/async_handler.hpp>

auto machine = abstraction::facade::discover();
abstraction::logging::AsyncOptions options;
options.program = "worker";
auto events = abstraction::logging::make_async_handler(
    machine.resolve_log({}, "local"), [machine] { return machine.resolve_log({}, "local"); }, options);
events->log(0, "worker started", {{"component", "worker"}});
```

`log` and `write` queue and return at once. The handler retries a failed
delivery with backoff, resolves again before each retry, drops the newest record
when its 1024-record queue is full, and delivers a gap record on recovery.
`counts()` reports the loss, and with no callbacks each transition is one line on
`std::cerr`. `close(timeout)` drains the queue.

## Run the host

Use an installed runtime and check `openabstractions start` followed by
`openabstractions status`. The facade example needs its resolver and a compatible
logging registration. A standalone `serve logging --out <path>` is an explicit
provider host; connect to it only with a deliberately selected endpoint and
independent server expectation. The host owns its output path.

A successful `Log` or `Write` is **one-way local submission**, not a persistence
receipt. `Write(const Record&)` is available when you already have a complete
schema-defined record; invalid `schema` values are refused by generated codecs.
The ordinary `Log` call avoids exposing these protocol details to applications.

## Build dependencies and scope

C++17 and CMake 3.16 are required. This package exports
`abstraction::logging_client` and depends on `abstraction_ipc`; the facade exports
`abstraction::facade_client` and depends on this package. CMake can use installed
packages or sibling source trees and does not download missing dependencies.

See [the logging contract](../CONTRACT.md) and [the Thrift definition](../logging.thrift).
The examples describe service-client source support. Release-specific evidence
qualifies installed packages and platforms; other capabilities have their own
contracts and client packages.
