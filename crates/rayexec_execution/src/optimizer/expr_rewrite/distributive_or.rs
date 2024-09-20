use crate::expr::Expression;
use rayexec_error::Result;

use super::ExpressionRewriteRule;

/// Tries to lift up AND expressions through OR expressions
///
/// '(a AND b) OR (a AND c) OR (a AND d) = a AND (b OR c OR d)'
#[derive(Debug)]
pub struct DistributiveOrRewrite;

impl ExpressionRewriteRule for DistributiveOrRewrite {
    fn rewrite(expressions: Vec<Expression>) -> Result<Vec<Expression>> {
        unimplemented!()
    }
}
