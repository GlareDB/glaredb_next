use hashbrown::HashMap;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;

use crate::functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction};

use super::catalog::CatalogTx;

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: String,
}

#[derive(Debug)]
pub struct CreateScalarFunction {
    pub name: String,
    pub implementation: Box<dyn GenericScalarFunction>,
}

#[derive(Debug)]
pub struct CreateAggregateFunction {
    pub name: String,
    pub implementation: Box<dyn GenericAggregateFunction>,
}

#[derive(Debug)]
pub enum CatalogEntry {
    Table(()),
    Function(FunctionEntry),
    External(()),
}

impl From<FunctionEntry> for CatalogEntry {
    fn from(value: FunctionEntry) -> Self {
        CatalogEntry::Function(value)
    }
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

pub trait Schema: Debug + Sync + Send {
    fn try_get_entry(&self, tx: &CatalogTx, name: &str) -> Result<Option<&CatalogEntry>>;

    fn try_get_scalar_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<&Box<dyn GenericScalarFunction>>> {
        match self.try_get_entry(tx, name)? {
            Some(CatalogEntry::Function(FunctionEntry { implementation, .. })) => {
                match implementation {
                    FunctionImpl::Scalar(scalar) => Ok(Some(scalar)),
                    _ => Ok(None),
                }
            }
            Some(_) => Err(RayexecError::new("Not a function")),
            None => Ok(None),
        }
    }

    fn try_get_aggregate_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<&Box<dyn GenericAggregateFunction>>> {
        match self.try_get_entry(tx, name)? {
            Some(CatalogEntry::Function(FunctionEntry { implementation, .. })) => {
                match implementation {
                    FunctionImpl::Aggregate(agg) => Ok(Some(agg)),
                    _ => Ok(None),
                }
            }
            Some(_) => Err(RayexecError::new("Not a function")),
            None => Ok(None),
        }
    }

    fn create_table(&mut self, tx: &CatalogTx, create: CreateTable) -> Result<()>;
    fn create_scalar_function(
        &mut self,
        tx: &CatalogTx,
        create: CreateScalarFunction,
    ) -> Result<()>;
    fn create_aggregate_function(
        &mut self,
        tx: &CatalogTx,
        create: CreateAggregateFunction,
    ) -> Result<()>;
}

/// A schema implementation that holds all items in memory.
///
////This may be intialized by reading a catalog from persistent storage, or
/// created on-demand.
#[derive(Debug)]
pub struct InMemorySchema {
    // TODO: OIDs
    // TODO: Seperate maps for funcs/tables
    entries: HashMap<String, CatalogEntry>,
}

impl Schema for InMemorySchema {
    fn try_get_entry(&self, _tx: &CatalogTx, name: &str) -> Result<Option<&CatalogEntry>> {
        Ok(self.entries.get(name))
    }

    fn create_table(&mut self, tx: &CatalogTx, create: CreateTable) -> Result<()> {
        unimplemented!()
    }

    fn create_scalar_function(
        &mut self,
        tx: &CatalogTx,
        create: CreateScalarFunction,
    ) -> Result<()> {
        self.insert_entry(
            tx,
            create.name.clone(),
            FunctionEntry {
                name: create.name,
                implementation: FunctionImpl::Scalar(create.implementation),
            },
        )
    }

    fn create_aggregate_function(
        &mut self,
        tx: &CatalogTx,
        create: CreateAggregateFunction,
    ) -> Result<()> {
        self.insert_entry(
            tx,
            create.name.clone(),
            FunctionEntry {
                name: create.name,
                implementation: FunctionImpl::Aggregate(create.implementation),
            },
        )
    }
}

impl InMemorySchema {
    pub fn new() -> Self {
        InMemorySchema {
            entries: HashMap::new(),
        }
    }

    fn insert_entry(
        &mut self,
        _tx: &CatalogTx,
        name: impl Into<String>,
        ent: impl Into<CatalogEntry>,
    ) -> Result<()> {
        let name = name.into();
        let ent = ent.into();

        // TODO: IF NOT EXISTS, etc
        if self.entries.contains_key(&name) {
            return Err(RayexecError::new(format!(
                "Duplicated entry for name '{name}'"
            )));
        }

        self.entries.insert(name, ent);

        Ok(())
    }
}
