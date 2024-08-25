use std::sync::Arc;

use rayexec_bullet::{
    array::Array, batch::Batch, compute::cast::array::cast_array, datatype::DataType,
};
use rayexec_error::{RayexecError, Result};

use crate::functions::scalar::PlannedScalarFunction;

use super::PhysicalScalarExpression;

#[derive(Debug, Clone)]
pub struct PhysicalScalarFunctionExpr {
    pub function: Box<dyn PlannedScalarFunction>,
    pub inputs: Vec<PhysicalScalarExpression>,
}

impl PhysicalScalarFunctionExpr {
    pub fn eval(&self, batch: &Batch) -> Result<Arc<Array>> {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.eval(batch))
            .collect::<Result<Vec<_>>>()?;
        let refs: Vec<_> = inputs.iter().collect(); // Can I not?
        let mut out = self.function.execute(&refs)?;

        // If function is provided no input, it's expected to return an
        // array of length 1. We extend the array here so that it's the
        // same size as the rest.
        if refs.is_empty() {
            let scalar = out
                .scalar(0)
                .ok_or_else(|| RayexecError::new("Missing scalar at index 0"))?;

            // TODO: Probably want to check null, and create the
            // appropriate array type since this will create a
            // NullArray, and not the type we're expecting.
            out = scalar.as_array(batch.num_rows());
        }

        // TODO: Do we want to Arc here? Should we allow batches to be mutable?

        Ok(Arc::new(out))
    }
}
