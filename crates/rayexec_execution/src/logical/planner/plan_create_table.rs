use crate::logical::{
    binder::{bind_context::BindContext, bind_create_table::BoundCreateTable},
    logical_create::LogicalCreateTable,
    operator::{LocationRequirement, LogicalOperator, Node},
    planner::plan_query::QueryPlanner,
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct CreateTablePlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> CreateTablePlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        CreateTablePlanner { bind_context }
    }

    pub fn plan(&self, create: BoundCreateTable) -> Result<LogicalOperator> {
        let children = if let Some(source) = create.source {
            let planner = QueryPlanner::new(self.bind_context);
            vec![planner.plan(source)?]
        } else {
            Vec::new()
        };

        Ok(LogicalOperator::CreateTable(Node {
            node: LogicalCreateTable {
                catalog: create.catalog,
                schema: create.schema,
                name: create.name,
                columns: create.columns,
                on_conflict: create.on_conflict,
            },
            location: LocationRequirement::ClientLocal,
            children,
        }))
    }
}
