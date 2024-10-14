//! File metadata

use super::row_group::RowGroupMetadata;
use super::schema::Schema;
use crate::thrift_gen::{ColumnOrder, KeyValue};

/// File metadata for a parquet file.
#[derive(Debug, Clone, PartialEq)]
pub struct FileMetadata {
    pub version: i32,
    pub schema: Schema,
    pub num_rows: i64,
    pub row_groups: Vec<RowGroupMetadata>,
    pub created_by: Option<String>,
    pub key_value_metadata: Option<Vec<KeyValue>>,
    pub column_orders: Option<Vec<ColumnOrder>>,
}
