use std::borrow::Cow;

use crate::encoding::Encoding;
use crate::statistics::Statistics;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataPageHeaderV1 {
    pub num_values: i32,
    pub encoding: Encoding,
    pub definition_level_encoding: Encoding,
    pub repetition_level_encoding: Encoding,
    pub statistics: Option<Statistics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPageHeaderV2 {
    pub num_values: i32,
    pub num_nulls: i32,
    pub num_rows: i32,
    pub encoding: Encoding,
    pub definition_levels_byte_length: i32,
    pub repetition_levels_byte_length: i32,
    pub is_compressed: Option<bool>,
    pub statistics: Option<Statistics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataPageHeader {
    V1(DataPageHeaderV1),
    V2(DataPageHeaderV2),
}

impl DataPageHeader {
    pub fn get_v2(&self) -> Option<&DataPageHeaderV2> {
        match self {
            Self::V1(_) => None,
            Self::V2(v2) => Some(v2),
        }
    }
}

/// Holds uncompressed data for a page alongside some metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPage<'a> {
    pub header: DataPageHeader,
    pub buffer: Cow<'a, [u8]>,
}
