use crate::logical::{
    binder::{bind_context::BindContext, bind_query::BoundQuery},
    logical_scan::{LogicalScan, ScanSource},
    operator::{LocationRequirement, LogicalOperator, Node},
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
            BoundQuery::Values(values) => {
                let table = self.bind_context.get_table(values.expressions_table)?;

                Ok(LogicalOperator::Scan(Node {
                    node: LogicalScan {
                        table_ref: values.expressions_table,
                        types: table.column_types.clone(),
                        names: table.column_names.clone(),
                        projection: (0..table.num_columns()).collect(),
                        source: ScanSource::ExpressionList { rows: values.rows },
                    },
                    location: LocationRequirement::Any,
                    children: Vec::new(),
                }))
            }
        }
    }
}
