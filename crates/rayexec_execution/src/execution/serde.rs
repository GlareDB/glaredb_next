//! (De)serialization logic for physical operators.

use serde::{Serialize, Serializer};

use crate::{database::DatabaseContext, execution::operators::create_schema::PhysicalCreateSchema};

use super::operators::{ExecutableOperator, PhysicalOperator};

#[derive(Debug)]
pub struct OperatorSerializer<'a>(&'a PhysicalOperator);

impl<'a> Serialize for OperatorSerializer<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub struct PhysicalOperatorDeserializer<'a> {
    pub context: &'a DatabaseContext,
}
