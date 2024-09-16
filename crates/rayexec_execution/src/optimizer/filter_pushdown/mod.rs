use rayexec_error::Result;

use crate::logical::{binder::bind_context::BindContext, operator::LogicalOperator};

use super::OptimizeRule;

#[derive(Debug, Clone)]
pub struct FilterPushdownRule {}

impl OptimizeRule for FilterPushdownRule {
    fn optimize(
        &self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        unimplemented!()
    }
}
