pub mod filter_pushdown;
pub mod location;

use crate::logical::{binder::bind_context::BindContext, operator::LogicalOperator};
use filter_pushdown::FilterPushdownRule;
use rayexec_error::Result;

#[derive(Debug)]
pub struct Optimizer {}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    pub fn new() -> Self {
        Optimizer {}
    }

    /// Run a logical plan through the optimizer.
    pub fn optimize(
        &self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut rule = FilterPushdownRule::default();
        let optimized = rule.optimize(bind_context, plan)?;

        // let rule = LocationRule {};
        // let optimized = rule.optimize(bind_context, optimized)?;

        Ok(optimized)
    }
}

pub trait OptimizeRule {
    /// Apply an optimization rule to the logical plan.
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator>;
}
