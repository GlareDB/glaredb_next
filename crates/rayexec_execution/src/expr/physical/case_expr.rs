use std::{fmt, sync::Arc};

use rayexec_bullet::{array::Array, batch::Batch, bitmap::Bitmap, compute::filter};
use rayexec_error::{RayexecError, Result};

use super::PhysicalScalarExpression;

#[derive(Debug, Clone)]
pub struct PhyscialWhenThen {
    pub when: PhysicalScalarExpression,
    pub then: PhysicalScalarExpression,
}

impl fmt::Display for PhyscialWhenThen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN {}", self.when, self.then)
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCaseExpr {
    pub cases: Vec<PhyscialWhenThen>,
    pub else_expr: Option<Box<PhysicalScalarExpression>>,
}

impl PhysicalCaseExpr {
    pub fn eval(&self, batch: &Batch) -> Result<Arc<Array>> {
        for case in &self.cases {
            // let filtered = batch.columns().iter().map(|c| filter::filter(c.as_ref(), &nc))

            let bools = match case.when.eval(batch)?.as_ref() {
                Array::Boolean(bools) => bools,
                other => {
                    return Err(RayexecError::new(format!(
                        "WHEN expr return non-boolean results: {}",
                        other.datatype()
                    )))
                }
            };
        }

        unimplemented!()
    }
}

impl fmt::Display for PhysicalCaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE ")?;
        for case in &self.cases {
            write!(f, "{}", case)?;
        }

        if let Some(else_expr) = self.else_expr.as_ref() {
            write!(f, "ELSE {}", else_expr)?;
        }

        Ok(())
    }
}
