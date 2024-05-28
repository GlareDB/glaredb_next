//! Various create messages/structs.
use crate::functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction};
use rayexec_bullet::field::DataType;

/// Behavior on create conflict.
#[derive(Debug, Default, Clone, Copy)]
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

#[derive(Debug, Clone)]
pub struct CreateTableInfo {
    pub name: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<DataType>,
    pub on_conflict: OnConflict,
}

#[derive(Debug, Clone)]
pub struct CreateSchemaInfo {
    pub name: String,
    pub on_conflict: OnConflict,
}

#[derive(Debug)]
pub struct CreateScalarFunctionInfo {
    pub name: String,
    pub implementation: Box<dyn GenericScalarFunction>,
    pub on_conflict: OnConflict,
}

#[derive(Debug)]
pub struct CreateAggregateFunctionInfo {
    pub name: String,
    pub implementation: Box<dyn GenericAggregateFunction>,
    pub on_conflict: OnConflict,
}
