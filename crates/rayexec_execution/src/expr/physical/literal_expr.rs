use std::sync::Arc;

use rayexec_bullet::{array::Array, batch::Batch, scalar::OwnedScalarValue};
use rayexec_error::Result;

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalLiteralExpr {
    pub literal: OwnedScalarValue,
}

impl PhysicalLiteralExpr {
    pub fn eval(&self, batch: &Batch) -> Result<Arc<Array>> {
        Ok(Arc::new(self.literal.as_array(batch.num_rows())))
    }
}
