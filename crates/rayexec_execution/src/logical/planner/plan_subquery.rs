use crate::{
    expr::{subquery_expr::SubqueryExpr, Expression},
    logical::{binder::bind_context::BindContext, operator::LogicalOperator},
};
use rayexec_error::{not_implemented, Result};

#[derive(Debug)]
pub struct SubqueryPlanner;

impl SubqueryPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        expr: &mut Expression,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        self.plan_inner(expr, &mut plan)?;
        Ok(plan)
    }

    fn plan_inner(&self, expr: &mut Expression, plan: &mut LogicalOperator) -> Result<()> {
        match expr {
            Expression::Subquery(subquery) => not_implemented!("subquery plan"),
            other => other.for_each_child_mut(&mut |expr| {
                self.plan_inner(expr, plan)?;
                Ok(())
            })?,
        }

        Ok(())
    }

    fn plan_uncorrelated(
        &self,
        subquery: &mut SubqueryExpr,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        unimplemented!()
    }
}
