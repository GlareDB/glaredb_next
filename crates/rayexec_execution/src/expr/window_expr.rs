use crate::functions::aggregate::PlannedAggregateFunction;

use super::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct WindowExpr {
    pub agg: Box<dyn PlannedAggregateFunction>,
    pub inputs: Vec<Expression>,
    pub filter: Box<Expression>,
    pub partition_by: Vec<Expression>,
}
