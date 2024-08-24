use crate::logical::binder::bind_context::TableScopeRef;

/// Reference to a column in a query.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnExpr {
    /// Scope this column is in.
    pub table_scope: TableScopeRef,
    /// Column index within the table.
    pub column: usize,
}
