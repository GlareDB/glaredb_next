use rayexec_error::Result;

use crate::logical::{
    binder::{bind_context::BindContext, bound_statement::BoundStatement},
    operator::LogicalOperator,
};

use super::plan_query::QueryPlanner;

#[derive(Debug)]
pub struct StatementPlanner<'a> {
    pub bind_context: &'a BindContext, // TODO: Need mut?
}

impl<'a> StatementPlanner<'a> {
    pub fn plan(&self, statement: BoundStatement) -> Result<LogicalOperator> {
        match statement {
            BoundStatement::Query(query) => {
                let planner = QueryPlanner {
                    bind_context: self.bind_context,
                };
                planner.plan(query)
            }
        }
    }
}
