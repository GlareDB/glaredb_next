pub mod planner;

pub mod cast_expr;
pub mod column_expr;
pub mod literal_expr;
pub mod scalar_function_expr;

use std::sync::Arc;

use cast_expr::PhysicalCastExpr;
use column_expr::PhysicalColumnExpr;
use literal_expr::PhysicalLiteralExpr;
use rayexec_bullet::{array::Array, batch::Batch};
use rayexec_error::Result;
use scalar_function_expr::PhysicalScalarFunctionExpr;

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
