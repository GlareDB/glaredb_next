use crate::{
    expr::Expression,
    logical::{
        binder::bind_context::BindContext, expr::LogicalExpression, operator::LogicalOperator,
    },
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct SubqueryPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> SubqueryPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        SubqueryPlanner { bind_context }
    }

    pub fn plan(&self, expr: &mut Expression, plan: LogicalOperator) -> Result<LogicalOperator> {
        unimplemented!()
    }
}
