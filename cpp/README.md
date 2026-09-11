# Logging from C++

Send structured events to the machine's logging host. The C++ client contains no
file sink, store, listener or local fallback. `Logger::Log` supplies schema and
timestamp; applications supply the event.

**Development source:** this path is not a claim that a published package or an
OS-installed service is available. C++ clients use the shared IPC runtime; the
host is Go.

## Through the facade

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

## Run the host

In another process, using a locally built executable:

```sh
openabstractions serve logging --out ./logs/records.jsonl
```

The foreground host owns `--out`; the C++ application never opens that file.
This command does not register an OS service. Both sides use the default framed
endpoint unless `ABSTRACTION_LOG_ENDPOINT` overrides it. The host also accepts
`--endpoint`. This endpoint is distinct from the legacy
`ABSTRACTION_LOG_SERVICE` raw-record stream.

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
The examples describe the new service-client path only. Other facade capabilities
remain on their legacy implementations; this page makes no all-platform or
installed-service conformance claim.
