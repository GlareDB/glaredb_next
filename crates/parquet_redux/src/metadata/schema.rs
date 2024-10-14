use crate::types::{GroupType, ParquetType};

/// Schema of a parquet file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// Fields in the schema.
    pub fields: Vec<ParquetType>,
}
