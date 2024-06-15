use rayexec_error::{RayexecError, Result};

use crate::{expr::scalar::UnaryOperator, logical::operator::LogicalExpression};

/// Evaluate constant expressions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConstEval {}

impl ConstEval {
    pub fn fold(&self, expr: LogicalExpression) -> Result<LogicalExpression> {
        unimplemented!()
    }
}
