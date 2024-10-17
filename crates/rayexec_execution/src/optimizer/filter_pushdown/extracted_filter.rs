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
        fn inner(child: &Expression, refs: &mut HashSet<TableRef>) {
            match child {
                Expression::Column(col) => {
                    refs.insert(col.table_scope);
                }
                other => other
                    .for_each_child(&mut |child| {
                        inner(child, refs);
                        Ok(())
                    })
                    .expect("getting table refs to not fail"),
            }
        }

        let mut refs = HashSet::new();
        inner(&expr, &mut refs);

        ExtractedFilter {
            filter: expr,
            tables_refs: refs,
        }
    }

    /// Checks if this filter is a candidate to be used for an equality join.
    ///
    /// A candidate requires that the filter only reference two table refs, is
    /// an Eq comparison, and the each side references one of the two refs and
    /// without overlap.
    pub fn is_equality_join_candidate(&self) -> bool {
        if self.tables_refs.len() != 2 {
            return false;
        }

        let (left, right) = match &self.filter {
            Expression::Comparison(cmp) if cmp.op == ComparisonOperator::Eq => {
                (&cmp.left, &cmp.right)
            }
            _ => return false,
        };

        let left_refs = left.get_table_references();
        let right_refs = right.get_table_references();

        if left_refs.len() > 1 || right_refs.len() > 1 {
            return false;
        }

        let different = left_refs != right_refs;

        different
    }
}

impl Hash for ExtractedFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.filter.hash(state)
    }
}
