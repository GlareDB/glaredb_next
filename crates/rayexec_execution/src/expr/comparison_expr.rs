use crate::functions::scalar::{comparison, ScalarFunction};

use super::Expression;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl ComparisonOperator {
    pub fn scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::Eq => &comparison::Eq,
            Self::NotEq => &comparison::Neq,
            Self::Lt => &comparison::Lt,
            Self::LtEq => &comparison::LtEq,
            Self::Gt => &comparison::Gt,
            Self::GtEq => &comparison::GtEq,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComparisonExpr {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub op: ComparisonOperator,
}
