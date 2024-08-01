//! (De)serialization logic for physical operators.

use serde::Serialize;

use super::operators::PhysicalOperator;

#[derive(Debug)]
pub struct OperatorSerializer<'a>(&'a dyn PhysicalOperator);

// impl<'a> Serialize for OperatorSerializer
