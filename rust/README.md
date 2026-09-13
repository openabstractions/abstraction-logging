# Generated Rust logging protocol package

`abstraction-logging-api` compiles the generated `../rs/abstraction/logging/rec.rs`
source directly. It exports Record/Page types, Sink/HistoryReader traits and
clients accepting the generated FrameTransport trait. It includes no transport or
provider. Keep `rust/` and `rs/` as siblings when using this source package.

The shared IPC adapter currently lives in the optional
`abstraction-facade-service` source crate. Version 0.0.0 is a development source
version; no registry release is claimed.
