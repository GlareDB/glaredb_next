use std::fmt::Debug;

use super::PrimitiveStorage;

pub trait OffsetIndex: Debug + Clone + Copy + PartialEq + Eq {
    fn get(start: Self, end: Self, slice: &[u8]) -> Option<&[u8]>;
}

impl OffsetIndex for i32 {
    fn get(start: Self, end: Self, slice: &[u8]) -> Option<&[u8]> {
        slice.get((start as usize)..(end as usize))
    }
}

impl OffsetIndex for i64 {
    fn get(start: Self, end: Self, slice: &[u8]) -> Option<&[u8]> {
        slice.get((start as usize)..(end as usize))
    }
}

/// Backing storage for multiple variable length values stored in a contiguous
/// vector.
///
/// This should be the backing storage for binary and (most) string data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContiguousVarlenStorage<O> {
    /// Offsets into the data buffer. The first value should be 0.
    pub(crate) offsets: PrimitiveStorage<O>,
    /// The data buffers being indexed into.
    pub(crate) data: PrimitiveStorage<u8>,
}

impl<O: OffsetIndex> ContiguousVarlenStorage<O> {
    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        let start = self.offsets.as_ref().get(idx)?;
        let end = self.offsets.as_ref().get(idx + 1)?;

        O::get(*start, *end, self.data.as_ref())
    }
}
