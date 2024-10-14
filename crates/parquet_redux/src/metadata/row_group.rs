//! Row group metadata.

use super::column_chunk::ColumnChunkMetadata;
use super::schema::Schema;
use crate::thrift_gen::SortingColumn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowGroupMetadata {
    pub columns: Vec<ColumnChunkMetadata>,
    pub num_rows: i64,
    pub sorting_columns: Option<Vec<SortingColumn>>,
    pub total_byte_size: i64,
    pub schema_descr: Schema,
    /// Offset from beginning of file.
    pub file_offset: Option<i64>,
    /// Row group ordinal in the file.
    pub ordinal: Option<i16>,
}
