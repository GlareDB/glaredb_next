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

    pub fn iter(&self) -> SharedHeapIter {
        SharedHeapIter {
            inner: self.blobs.iter(),
        }
    }
}

#[derive(Debug)]
pub struct SharedHeapIter<'a> {
    inner: std::slice::Iter<'a, Bytes>,
}

impl<'a> Iterator for SharedHeapIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|b| b.as_ref())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a> ExactSizeIterator for SharedHeapIter<'a> {}
