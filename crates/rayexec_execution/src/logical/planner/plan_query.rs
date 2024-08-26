use crate::logical::{
    binder::{bind_context::BindContext, bind_query::BoundQuery},
    operator::LogicalOperator,
    planner::plan_select::SelectPlanner,
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct QueryPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        QueryPlanner { bind_context }
    }

    pub fn plan(&self, query: BoundQuery) -> Result<LogicalOperator> {
        match query {
            BoundQuery::Select(select) => {
                let planner = SelectPlanner {
                    bind_context: self.bind_context,
                };
                planner.plan(select)
            }
        }
    }
}
