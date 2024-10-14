//! File metadata.
use rayexec_error::{Result, ResultExt};
use thrift::protocol::TCompactInputProtocol;

use super::row_group::RowGroupMetadata;
use super::schema::Schema;
use crate::thrift_ext::TSerializable;
use crate::thrift_gen;

/// File metadata for a parquet file.
#[derive(Debug, Clone, PartialEq)]
pub struct FileMetadata {
    pub version: i32,
    pub schema: Schema,
    pub num_rows: i64,
    pub row_groups: Vec<RowGroupMetadata>,
    pub created_by: Option<String>,
    pub key_value_metadata: Option<Vec<thrift_gen::KeyValue>>,
    pub column_orders: Option<Vec<thrift_gen::ColumnOrder>>,
}

impl FileMetadata {
    /// Try to decode the metadata from a byte buffer.
    ///
    /// This should come from the end of a parquet file, and be the exact size
    /// of the metadata.
    pub fn try_decode(buf: &[u8]) -> Result<Self> {
        let mut input = TCompactInputProtocol::new(buf);
        let file_meta = thrift_gen::FileMetaData::read_from_in_protocol(&mut input)
            .context("Failed to read file metadata")?;

        let schema = Schema::try_from_thrift(&file_meta.schema)?;

        unimplemented!()
    }
}
