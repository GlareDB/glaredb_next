use crate::logical::{
    binder::{bind_context::BindContext, bind_insert::BoundInsert, bind_query::BoundQuery},
    logical_insert::LogicalInsert,
    logical_project::LogicalProject,
    operator::{LocationRequirement, LogicalOperator, Node},
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

    pub fn plan(&self, insert: BoundInsert) -> Result<LogicalOperator> {
        let planner = QueryPlanner::new(self.bind_context);
        let mut source = planner.plan(insert.source)?;

        if let Some(projections) = insert.projections {
            source = LogicalOperator::Project(Node {
                node: LogicalProject { projections },
                location: LocationRequirement::Any,
                children: vec![source],
                input_table_refs: None,
            })
        }

        Ok(LogicalOperator::Insert(Node {
            node: LogicalInsert {
                catalog: insert.table.catalog,
                schema: insert.table.schema,
                table: insert.table.entry,
            },
            location: insert.table_location,
            children: vec![source],
            input_table_refs: None,
        }))
    }
}
