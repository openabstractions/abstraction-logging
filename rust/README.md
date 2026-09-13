# Generated Rust logging protocol package

`abstraction-logging-api` compiles the generated `../rs/abstraction/logging/rec.rs`
source directly. It exports Record/Page types, Sink/HistoryReader traits and
clients accepting the generated FrameTransport trait. It includes no transport or
provider. Keep `rust/` and `rs/` as siblings when using this source package.

The shared native connector lives in the separate `abstraction-facade-native`
crate. Optional `abstraction-facade-logging` binds this protocol through the pure
facade core; neither the protocol nor pure core imports a native provider. Version 0.0.0 is a development source
version; no registry release is claimed.
