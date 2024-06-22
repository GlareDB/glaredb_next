use crate::logical::{expr::LogicalExpression, operator::LogicalOperator};
use rayexec_error::Result;

#[derive(Debug)]
pub struct SubqueryDecorrelator {}

impl SubqueryDecorrelator {
    pub fn plan_correlated(&mut self, expr: &mut LogicalExpression) -> Result<()> {
        match expr {
            expr @ LogicalExpression::Subquery(_) => {}
            _ => unimplemented!(),
        }

        unimplemented!()
    }
}
