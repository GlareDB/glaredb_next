//! Arrow IPC compatability.
//!
//! Spec: <https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc>
pub mod reader;

mod batch;
mod compression;
mod gen;
mod schema;

#[derive(Debug, Clone)]
pub struct IpcConfig {}
