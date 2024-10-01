use bytes::Bytes;

/// Back storage for shared byte buffers.
///
/// This mostly exists to allow us to use the byte blobs produced from parquet
/// directly with needing to copy it.
///
/// It's unknown if this will continue to exist long term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedHeapStorage {
    pub(crate) blobs: Vec<Bytes>,
}
