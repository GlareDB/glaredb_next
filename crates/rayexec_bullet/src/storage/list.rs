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
    pub fn single_list(array: Array) -> Self {
        let len = array.logical_len();

        ListStorage {
            metadata: vec![ListItemMetadata {
                offset: 0,
                len: len as i32,
            }]
            .into(),
            array,
        }
    }

    pub fn len(&self) -> usize {
        self.metadata.len()
    }
}
