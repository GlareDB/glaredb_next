use std::sync::Arc;

use rayexec_bullet::{array::Array, batch::Batch};
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone)]
pub struct PhysicalColumnExpr {
    pub idx: usize,
}

impl PhysicalColumnExpr {
    pub fn eval(&self, batch: &Batch) -> Result<Arc<Array>> {
        batch
            .column(self.idx)
            .ok_or_else(|| {
                RayexecError::new(format!(
                    "Tried to get column at index {} in a batch with {} columns",
                    self.idx,
                    batch.columns().len()
                ))
            })
            .cloned()
    }
}
