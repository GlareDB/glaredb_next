use rayexec_error::Result;

use crate::logical::{
    binder::{
        bind_attach::{BoundAttach, BoundDetach},
        bind_context::BindContext,
        bind_statement::BoundStatement,
    },
    operator::LogicalOperator,
};

use super::{
    plan_create_table::CreateTablePlanner, plan_insert::InsertPlanner, plan_query::QueryPlanner,
};

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
            BoundStatement::SetVar(plan) => Ok(LogicalOperator::SetVar(plan)),
            BoundStatement::ShowVar(plan) => Ok(LogicalOperator::ShowVar(plan)),
            BoundStatement::ResetVar(plan) => Ok(LogicalOperator::ResetVar(plan)),
            BoundStatement::Attach(BoundAttach::Database(plan)) => {
                Ok(LogicalOperator::AttachDatabase(plan))
            }
            BoundStatement::Detach(BoundDetach::Database(plan)) => {
                Ok(LogicalOperator::DetachDatabase(plan))
            }
            BoundStatement::Drop(plan) => Ok(LogicalOperator::Drop(plan)),
            BoundStatement::Insert(insert) => InsertPlanner::new(self.bind_context).plan(insert),
            BoundStatement::CreateSchema(plan) => Ok(LogicalOperator::CreateSchema(plan)),
            BoundStatement::CreateTable(create) => {
                CreateTablePlanner::new(self.bind_context).plan(create)
            }
            BoundStatement::Describe(plan) => Ok(LogicalOperator::Describe(plan)),
        }
    }
}
