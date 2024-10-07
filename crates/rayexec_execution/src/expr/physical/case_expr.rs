use std::{borrow::Cow, fmt};

use rayexec_bullet::{
    array::Array,
    batch::Batch,
};
use rayexec_error::{not_implemented, Result};

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
    pub else_expr: Box<PhysicalScalarExpression>,
}

impl PhysicalCaseExpr {
    pub fn eval<'a>(&self, batch: &'a Batch) -> Result<Cow<'a, Array>> {
        not_implemented!("CASE")
    }
}

impl fmt::Display for PhysicalCaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE ")?;
        for case in &self.cases {
            write!(f, "{} ", case)?;
        }
        write!(f, "ELSE {}", self.else_expr)?;

        Ok(())
    }
}
