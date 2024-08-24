use super::Expression;

/// <input> BETWEEN <lower> AND <upper>
#[derive(Debug, Clone, PartialEq)]
pub struct BetweenExpr {
    pub lower: Box<Expression>,
    pub upper: Box<Expression>,
    pub input: Box<Expression>,
}
