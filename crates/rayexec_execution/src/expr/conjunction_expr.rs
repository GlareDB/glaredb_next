use crate::functions::scalar::{boolean, ScalarFunction};
use std::fmt;

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

impl fmt::Display for Conjunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConjunctionExpr {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub conjunction: Conjunction,
}

impl fmt::Display for ConjunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.right, self.conjunction)
    }
}
