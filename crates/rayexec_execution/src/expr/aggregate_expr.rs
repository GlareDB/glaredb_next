use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use crate::{
    functions::aggregate::PlannedAggregateFunction, logical::binder::bind_context::BindContext,
};

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

impl AggregateExpr {
    pub fn datatype(&self, bind_context: &BindContext) -> Result<DataType> {
        unimplemented!()
    }
}
