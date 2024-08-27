use crate::logical::binder::bind_context::TableRef;
use std::fmt;

/// Reference to a column in a query.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnExpr {
    /// Scope this column is in.
    pub table_scope: TableRef,
    /// Column index within the table.
    pub column: usize,
}

impl fmt::Display for ColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}.{}]", self.table_scope, self.column)
    }
}
