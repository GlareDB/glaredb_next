use rayexec_bullet::field::Field;
use rayexec_error::{RayexecError, Result};

use crate::functions::{
    aggregate::AggregateFunction, scalar::ScalarFunction, table::TableFunction,
};

#[derive(Debug)]
pub struct CatalogEntry {
    pub oid: u32,
    pub name: String,
    pub entry: CatalogEntryInner,
}

#[derive(Debug)]
pub enum CatalogEntryInner {
    Table(TableEntry),
    Schema(SchemaEntry),
    ScalarFunction(ScalarFunctionEntry),
    AggregateFunction(AggregateFunctionEntry),
    TableFunction(TableFunctionEntry),
    // TODO: COPY TO function
}

#[derive(Debug)]
pub struct ScalarFunctionEntry {
    pub function: Box<dyn ScalarFunction>,
}

#[derive(Debug)]
pub struct AggregateFunctionEntry {
    pub function: Box<dyn AggregateFunction>,
}

#[derive(Debug)]
pub struct TableFunctionEntry {
    pub function: Box<dyn TableFunction>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableEntry {
    pub columns: Vec<Field>,
}

#[derive(Debug)]
pub struct SchemaEntry {}

impl CatalogEntry {
    pub fn try_as_table_entry(&self) -> Result<&TableEntry> {
        match &self.entry {
            CatalogEntryInner::Table(ent) => Ok(ent),
            _ => return Err(RayexecError::new("Entry not a table")),
        }
    }

    pub fn try_as_schema_entry(&self) -> Result<&SchemaEntry> {
        match &self.entry {
            CatalogEntryInner::Schema(ent) => Ok(ent),
            _ => return Err(RayexecError::new("Entry not a schema")),
        }
    }

    pub fn try_as_scalar_function_entry(&self) -> Result<&ScalarFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::ScalarFunction(ent) => Ok(ent),
            _ => return Err(RayexecError::new("Entry not a scalar function")),
        }
    }

    pub fn try_as_aggregate_function_entry(&self) -> Result<&AggregateFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::AggregateFunction(ent) => Ok(ent),
            _ => return Err(RayexecError::new("Entry not an aggregate function")),
        }
    }

    pub fn try_as_table_function_entry(&self) -> Result<&TableFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::TableFunction(ent) => Ok(ent),
            _ => return Err(RayexecError::new("Entry not a table function")),
        }
    }
}
