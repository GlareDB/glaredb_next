use crate::functions::scalar::PlannedScalarFunction;

use super::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct ScalarFunctionExpr {
    pub function: Box<dyn PlannedScalarFunction>,
    pub inputs: Vec<Expression>,
}
