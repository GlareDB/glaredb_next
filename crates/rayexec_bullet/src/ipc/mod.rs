//! Arrow IPC compatability.
//!
//! Spec: <https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc>
pub mod reader;
pub mod writer;

mod batch;
mod compression;
mod gen;
mod schema;

#[derive(Debug, Clone)]
pub struct IpcConfig {}
