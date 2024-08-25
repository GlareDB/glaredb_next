use crate::{
    expr::Expression,
    logical::{binder::bind_context::BindContext, operator::LogicalOperator},
};
use rayexec_error::{not_implemented, Result};

#[derive(Debug)]
pub struct SubqueryPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> SubqueryPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        SubqueryPlanner { bind_context }
    }

    pub fn plan(
        &self,
        expr: &mut Expression,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        self.plan_inner(expr, &mut plan)?;
        Ok(plan)
    }

    fn plan_inner(&self, expr: &mut Expression, plan: &mut LogicalOperator) -> Result<()> {
        match expr {
            Expression::Subquery(_subquery) => not_implemented!("subquery plan"),
            other => other.for_each_child_mut(&mut |expr| {
                self.plan_inner(expr, plan)?;
                Ok(())
            })?,
        }

        Ok(())
    }
}
