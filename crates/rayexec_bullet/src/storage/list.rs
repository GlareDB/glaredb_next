use super::PrimitiveStorage;
use crate::array::Array;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListItemMetadata {
    pub offset: i32,
    pub len: i32,
}

#[derive(Debug)]
pub struct ListStorage {
    pub(crate) metadata: PrimitiveStorage<ListItemMetadata>,
    pub(crate) array: Array,
}
