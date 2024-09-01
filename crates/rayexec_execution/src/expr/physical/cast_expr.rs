use std::fmt;
use std::sync::Arc;

use rayexec_bullet::{
    array::Array, batch::Batch, compute::cast::array::cast_array, datatype::DataType,
};
use rayexec_error::Result;

use super::PhysicalScalarExpression;

#[derive(Debug, Clone)]
pub struct PhysicalCastExpr {
    pub to: DataType,
    pub expr: Box<PhysicalScalarExpression>,
}

impl PhysicalCastExpr {
    pub fn eval(&self, batch: &Batch) -> Result<Arc<Array>> {
        let input = self.expr.eval(batch)?;
        let out = cast_array(&input, &self.to)?;
        Ok(Arc::new(out))
    }
}

impl fmt::Display for PhysicalCastExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} TO {})", self.expr, self.to)
    }
}
