//! Various create messages/structs.
use crate::functions::{aggregate::AggregateFunction, scalar::ScalarFunction};
use rayexec_bullet::field::Field;
use serde::{Deserialize, Serialize};

/// Behavior on create conflict.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnConflict {
    /// Ignore and return ok.
    ///
    /// CREATE IF NOT EXIST
    Ignore,

    /// Replace the original entry.
    ///
    /// CREATE OR REPLACE
    Replace,

    /// Error on conflict.
    #[default]
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableInfo {
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateSchemaInfo {
    pub name: String,
    pub on_conflict: OnConflict,
}

#[derive(Debug, PartialEq)]
pub struct CreateScalarFunctionInfo {
    pub name: String,
    pub implementation: Box<dyn ScalarFunction>,
    pub on_conflict: OnConflict,
}

#[derive(Debug, PartialEq)]
pub struct CreateAggregateFunctionInfo {
    pub name: String,
    pub implementation: Box<dyn AggregateFunction>,
    pub on_conflict: OnConflict,
}
