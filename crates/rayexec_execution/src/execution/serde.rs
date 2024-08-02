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
        let seq = serializer.serialize_seq(Some(2))?;
        match self.0 {
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub struct PhysicalOperatorDeserializer<'a> {
    pub context: &'a DatabaseContext,
}
