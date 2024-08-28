pub mod planner;

pub mod cast_expr;
pub mod column_expr;
pub mod literal_expr;
pub mod scalar_function_expr;

use std::fmt;
use std::sync::Arc;

use cast_expr::PhysicalCastExpr;
use column_expr::PhysicalColumnExpr;
use literal_expr::PhysicalLiteralExpr;
use rayexec_bullet::{array::Array, batch::Batch, datatype::DataType};
use rayexec_error::Result;
use scalar_function_expr::PhysicalScalarFunctionExpr;

use crate::functions::aggregate::PlannedAggregateFunction;

#[derive(Debug, Clone)]
pub enum PhysicalScalarExpression {
    Cast(PhysicalCastExpr),
    Column(PhysicalColumnExpr),
    Literal(PhysicalLiteralExpr),
    ScalarFunction(PhysicalScalarFunctionExpr),
}

impl PhysicalScalarExpression {
    pub fn eval(&self, batch: &Batch) -> Result<Arc<Array>> {
        match self {
            Self::Cast(expr) => expr.eval(batch),
            Self::Column(expr) => expr.eval(batch),
            Self::Literal(expr) => expr.eval(batch),
            Self::ScalarFunction(expr) => expr.eval(batch),
        }
    }
}

impl fmt::Display for PhysicalScalarExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct PhysicalAggregateExpression {
    /// The function we'll be calling to produce the aggregate states.
    pub function: Box<dyn PlannedAggregateFunction>,
    /// Column expressions we're aggregating on.
    pub columns: Vec<PhysicalColumnExpr>,
    /// Output type of the aggregate.
    pub output_type: DataType,
    // TODO: Filter
}

impl PhysicalAggregateExpression {
    pub fn contains_column_idx(&self, column: usize) -> bool {
        self.columns.iter().any(|expr| expr.idx == column)
    }
}
