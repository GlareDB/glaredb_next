use std::collections::HashSet;

use crate::expr::{
    conjunction_expr::{ConjunctionExpr, ConjunctionOperator},
    Expression,
};
use rayexec_error::Result;

use super::ExpressionRewriteRule;

/// Tries to lift up AND expressions through OR expressions
///
/// '(a AND b) OR (a AND c) OR (a AND d) = a AND (b OR c OR d)'
#[derive(Debug)]
pub struct DistributiveOrRewrite;

impl ExpressionRewriteRule for DistributiveOrRewrite {
    fn rewrite(expression: Expression) -> Result<Expression> {
        unimplemented!()
        // match expression {
        //     Expression::Conjunction(ConjunctionExpr {
        //         left,
        //         right,
        //         op: ConjunctionOperator::Or,
        //     }) => {
        //         // Track initial candidates that we can pull out.
        //         let mut candidates: HashSet<&Expression> = HashSet::new();

        //         match left.as_ref() {
        //             Expression::Conjunction(ConjunctionExpr {
        //                 left,
        //                 right,
        //                 op: ConjunctionOperator::And,
        //             }) => {
        //                 // Both children of the AND are candidates.
        //                 candidates.insert(left.as_ref());
        //                 candidates.insert(right.as_ref());
        //             }
        //             other => {
        //                 // Not an AND expr. Entire expression is a candidate.
        //                 candidates.insert(other);
        //             }
        //         }

        //         unimplemented!()
        //     }
        //     other => Ok(other), // No change
        // }
    }
}
