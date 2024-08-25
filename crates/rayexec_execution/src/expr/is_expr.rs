use super::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsOperator {
    IsTrue,
    IsFalse,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IsExpr {
    pub op: IsOperator,
    pub input: Box<Expression>,
}
