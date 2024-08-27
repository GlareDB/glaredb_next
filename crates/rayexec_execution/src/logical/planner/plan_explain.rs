use crate::logical::{
    binder::{bind_context::BindContext, bind_explain::BoundExplain},
    logical_explain::LogicalExplain,
    operator::{LocationRequirement, LogicalNode, LogicalOperator},
    planner::plan_query::QueryPlanner,
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct ExplainPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> ExplainPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        ExplainPlanner { bind_context }
    }

    pub fn plan(&self, explain: BoundExplain) -> Result<LogicalOperator> {
        let planner = QueryPlanner::new(self.bind_context);
        let plan = planner.plan(explain.query)?;

        Ok(LogicalOperator::Explain(LogicalNode {
            node: LogicalExplain {
                analyze: explain.analyze,
                verbose: explain.verbose,
                format: explain.format,
                logical_unoptimized: Box::new(plan.clone()),
                logical_optimized: None,
            },
            location: LocationRequirement::Any,
            children: vec![plan],
        }))
    }
}
