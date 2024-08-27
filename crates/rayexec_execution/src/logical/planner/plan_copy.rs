use crate::logical::{
    binder::{
        bind_context::BindContext,
        bind_copy::{BoundCopyTo, BoundCopyToSource},
    },
    logical_copy::LogicalCopyTo,
    operator::{LocationRequirement, LogicalNode, LogicalOperator},
    planner::{plan_from::FromPlanner, plan_query::QueryPlanner},
};
use rayexec_error::Result;

pub struct CopyPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> CopyPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        CopyPlanner { bind_context }
    }

    pub fn plan(&self, copy_to: BoundCopyTo) -> Result<LogicalOperator> {
        let source = match copy_to.source {
            BoundCopyToSource::Query(query) => {
                let planner = QueryPlanner::new(self.bind_context);
                planner.plan(query)?
            }
            BoundCopyToSource::Table(table) => {
                let planner = FromPlanner::new(self.bind_context);
                planner.plan(table)?
            }
        };

        // Currently only support copying to local.

        Ok(LogicalOperator::CopyTo(LogicalNode {
            node: LogicalCopyTo {
                source_schema: copy_to.source_schema,
                location: copy_to.location,
                copy_to: copy_to.copy_to,
            },
            location: LocationRequirement::ClientLocal,
            children: vec![source],
        }))
    }
}
