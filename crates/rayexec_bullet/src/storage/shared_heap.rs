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

impl SharedHeapStorage {
    pub fn with_capacity(cap: usize) -> Self {
        SharedHeapStorage {
            blobs: Vec::with_capacity(cap),
        }
    }

    pub fn get(&self, idx: usize) -> Option<&Bytes> {
        self.blobs.get(idx)
    }

    pub fn push(&mut self, blob: impl Into<Bytes>) {
        self.blobs.push(blob.into())
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.blobs.iter().map(|b| b.as_ref())
    }
}
