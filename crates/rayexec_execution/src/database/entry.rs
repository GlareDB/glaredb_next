use rayexec_bullet::field::DataType;
use rayexec_error::{RayexecError, Result};

use crate::functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction};

#[derive(Debug)]
pub enum CatalogEntry {
    Table(TableEntry),
    Function(FunctionEntry),
    External(()),
}

impl CatalogEntry {
    pub fn try_as_function(&self) -> Result<&FunctionEntry> {
        match self {
            Self::Function(f) => Ok(f),
            _ => Err(RayexecError::new("Not a function")),
        }
    }
}

impl From<FunctionEntry> for CatalogEntry {
    fn from(value: FunctionEntry) -> Self {
        CatalogEntry::Function(value)
    }
}

#[derive(Debug)]
pub struct TableEntry {
    pub name: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<DataType>,
}

#[derive(Debug)]
pub struct FunctionEntry {
    pub name: String,
    pub implementation: FunctionImpl,
}

#[derive(Debug)]
pub enum FunctionImpl {
    Scalar(Box<dyn GenericScalarFunction>),
    Aggregate(Box<dyn GenericAggregateFunction>),
}
