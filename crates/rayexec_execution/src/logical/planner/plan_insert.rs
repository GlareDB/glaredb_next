use crate::logical::{
    binder::{bind_context::BindContext, bind_insert::BoundInsert},
    logical_insert::LogicalInsert,
    operator::{LogicalNode, LogicalOperator},
};
use rayexec_error::Result;

use super::plan_query::QueryPlanner;

#[derive(Debug)]
pub struct InsertPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> InsertPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        InsertPlanner { bind_context }
    }

    pub fn plan(&self, mut insert: BoundInsert) -> Result<LogicalOperator> {
        let planner = QueryPlanner::new(self.bind_context);
        let source = planner.plan(insert.source)?;

        Ok(LogicalOperator::Insert(LogicalNode {
            node: LogicalInsert {
                catalog: insert.table.catalog,
                schema: insert.table.schema,
                table: insert.table.entry,
            },
            location: insert.table_location,
            children: vec![source],
        }))
    }
}
