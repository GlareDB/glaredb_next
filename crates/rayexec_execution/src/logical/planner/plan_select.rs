use crate::logical::{
    binder::{bind_context::BindContext, bound_select::BoundSelect},
    operator::LogicalOperator,
    planner::{plan_from::FromPlanner, plan_subquery::SubqueryPlanner},
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct SelectPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> SelectPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        SelectPlanner { bind_context }
    }

    pub fn plan(&self, select: BoundSelect) -> Result<LogicalOperator> {
        // Handle FROM
        let mut plan = FromPlanner::new(self.bind_context).plan(select.from)?;

        // Handle WHERE
        if let Some(mut filter) = select.filter {
            let plan = SubqueryPlanner::new(self.bind_context).plan(&mut filter, plan)?;

            // Do it
            unimplemented!()
        }

        unimplemented!()
    }
}
