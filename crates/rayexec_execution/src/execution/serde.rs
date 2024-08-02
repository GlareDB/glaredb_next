//! (De)serialization logic for physical operators.

use serde::{Serialize, Serializer};

use crate::execution::operators::create_schema::PhysicalCreateSchema;

use super::operators::ExecutableOperator;

#[derive(Debug)]
pub struct OperatorSerializer<'a>(&'a dyn ExecutableOperator);

impl<'a> Serialize for OperatorSerializer<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0.operator_name() {
            _ => unimplemented!(),
        }
    }
}
