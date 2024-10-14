//! Column metadata.

use crate::types::PrimitiveType;

/// Physical type for leaf-level primitive columns.
///
/// Also includes the maximum definition and repetition levels required to
/// re-assemble nested data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDescriptor {
    /// The "leaf" primitive type of this column
    primitive_type: PrimitiveType,
    /// The maximum definition level for this column
    max_def_level: i16,
    /// The maximum repetition level for this column
    max_rep_level: i16,
}
