use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};
use crate::expr::conjunction_expr::ConjunctionOperator;
use crate::expr::Expression;
use crate::logical::binder::bind_context::TableRef;
use crate::logical::logical_join::ComparisonCondition;

/// An equality condition between a left and right table ref.
///
/// Can be used as a condition for an inner join.
#[derive(Debug, Clone)]
pub struct EqualityCondition {
    pub left: Expression,
    pub right: Expression,
    /// Table reference referenced in the left expression.
    pub left_ref: TableRef,
    /// Table reference referenced in the right expression.
    pub right_ref: TableRef,
}

impl EqualityCondition {
    /// Try to create an equality condition from the expression.
    ///
    /// Returns the original expression unchanged if an equality condition
    /// cannot be created.
    pub fn try_new(expr: Expression) -> Result<EqualityCondition, Expression> {
        match expr {
            Expression::Comparison(cmp) if cmp.op == ComparisonOperator::Eq => {
                let mut left_refs = cmp.left.get_table_references();
                let mut right_refs = cmp.right.get_table_references();

                // TODO: It is possible to have an equality condition that has
                // more than two refs.
                if left_refs.len() != 1 || right_refs.len() != 1 {
                    return Err(Expression::Comparison(cmp));
                }

                let left_ref = left_refs.drain().next().unwrap(); // Lengths checked above.
                let right_ref = right_refs.drain().next().unwrap();

                // Refs need to be distinct to be an equality condition.
                if left_ref == right_ref {
                    return Err(Expression::Comparison(cmp));
                }

                Ok(EqualityCondition {
                    left: *cmp.left,
                    right: *cmp.right,
                    left_ref,
                    right_ref,
                })
            }
            other => Err(other),
        }
    }

    pub fn into_comparision_condition(self) -> ComparisonCondition {
        ComparisonCondition {
            left: self.left,
            right: self.right,
            op: ComparisonOperator::Eq,
        }
    }

    pub fn into_expression(self) -> Expression {
        Expression::Comparison(ComparisonExpr {
            left: Box::new(self.left),
            right: Box::new(self.right),
            op: ComparisonOperator::Eq,
        })
    }
}
