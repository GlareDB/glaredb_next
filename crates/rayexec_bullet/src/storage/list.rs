use super::PrimitiveStorage;
use crate::array::Array;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata {
    pub offset: i32,
    pub len: i32,
}

#[derive(Debug, PartialEq)]
pub struct ListStorage {
    pub(crate) metadata: PrimitiveStorage<ListItemMetadata>,
    pub(crate) array: Array,
}

impl ListStorage {
    pub fn len(&self) -> usize {
        self.metadata.len()
    }
}
