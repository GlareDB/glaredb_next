use crate::functions::aggregate::PlannedAggregateFunction;

use super::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    /// The function.
    pub agg: Box<dyn PlannedAggregateFunction>,
    /// Input expressions to the aggragate.
    pub inputs: Vec<Expression>,
    /// Optional filter to the aggregate.
    pub filter: Option<Box<Expression>>,
}
