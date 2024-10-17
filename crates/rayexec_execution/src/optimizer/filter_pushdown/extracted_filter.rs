use std::collections::HashSet;
use std::hash::Hash;

use crate::expr::comparison_expr::ComparisonOperator;
use crate::expr::Expression;
use crate::logical::binder::bind_context::TableRef;

/// Holds a filtering expression and all table refs the expression references.
#[derive(Debug, PartialEq, Eq)]
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

    /// Tries to return [left, right] table refs for this filter if it can be
    /// used as an equality condition.
    ///
    /// Returns None if the filter cannot be used for an equality.
    ///
    /// A candidate requires that the filter only reference two table refs, is
    /// an Eq comparison, and the each side references one of the two refs and
    /// without overlap.
    pub fn try_get_tables_refs_for_equality(&self) -> Option<[TableRef; 2]> {
        // TODO: It's possible that a filter can have more than two table refs
        // and still be used for an equality join.
        if self.tables_refs.len() != 2 {
            return None;
        }

        let (left, right) = match &self.filter {
            Expression::Comparison(cmp) if cmp.op == ComparisonOperator::Eq => {
                (&cmp.left, &cmp.right)
            }
            _ => return None,
        };

        let mut left_refs = left.get_table_references();
        let mut right_refs = right.get_table_references();

        if left_refs.len() != 1 || right_refs.len() != 1 {
            return None;
        }

        let left_ref = left_refs.drain().next().unwrap();
        let right_ref = right_refs.drain().next().unwrap();

        if left_ref == right_ref {
            // Refs need to be different on both sides if we're trying to join
            // on them.
            return None;
        }

        Some([left_ref, right_ref])
    }
}

impl Hash for ExtractedFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.filter.hash(state)
    }
}
