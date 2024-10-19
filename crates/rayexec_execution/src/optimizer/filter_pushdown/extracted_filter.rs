use std::collections::HashSet;
use std::hash::Hash;

use crate::expr::comparison_expr::ComparisonOperator;
use crate::expr::Expression;
use crate::logical::binder::bind_context::TableRef;

/// Holds a filtering expression and all table refs the expression references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedFilter {
    /// The filter expression.
    pub filter: Expression,
    /// Tables refs this expression references.
    pub tables_refs: HashSet<TableRef>,
}

impl ExtractedFilter {
    pub fn from_expr(expr: Expression) -> Self {
        let refs = expr.get_table_references();
        ExtractedFilter {
            filter: expr,
            tables_refs: refs,
        }
    }

    pub fn into_expression(self) -> Expression {
        self.filter
    }
}

impl Hash for ExtractedFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.filter.hash(state)
    }
}
