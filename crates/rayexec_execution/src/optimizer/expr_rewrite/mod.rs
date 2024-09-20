pub mod distributive_or;

use crate::{
    expr::Expression,
    logical::{binder::bind_context::BindContext, operator::LogicalOperator},
};
use rayexec_error::Result;

use super::OptimizeRule;

pub trait ExpressionRewriteRule {
    fn rewrite(expression: Expression) -> Result<Expression>;
}

/// Rewrites expression to be amenable to futher optimization.
#[derive(Debug)]
pub struct ExpressionRewriter;

impl OptimizeRule for ExpressionRewriter {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        unimplemented!()
    }
}
