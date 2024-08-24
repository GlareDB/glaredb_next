use super::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct WhenThen {
    pub when: Expression,
    pub then: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpr {
    pub cases: Vec<WhenThen>,
    pub else_expr: Box<Expression>,
}
