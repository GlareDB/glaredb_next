use crate::functions::scalar::{boolean, ScalarFunction};

use super::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conjunction {
    And,
    Or,
}

impl Conjunction {
    pub fn scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::And => &boolean::And,
            Self::Or => &boolean::Or,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConjunctionExpr {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub conjunction: Conjunction,
}
