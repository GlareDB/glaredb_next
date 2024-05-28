use hashbrown::HashMap;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;

use crate::functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction};

use super::{
    catalog::CatalogTx,
    create::{CreateAggregateFunctionInfo, CreateScalarFunctionInfo, CreateTableInfo},
    ddl::SchemaModifier,
    entry::{CatalogEntry, FunctionEntry, FunctionImpl, TableEntry},
    table::DataTable,
};

pub trait Schema: Debug + Sync + Send {
    fn try_get_entry(&self, tx: &CatalogTx, name: &str) -> Result<Option<CatalogEntry>>;

    fn get_data_table(&self, tx: &CatalogTx, ent: &TableEntry) -> Result<Box<dyn DataTable>>;

    fn get_modifier(&self, tx: &CatalogTx) -> Result<Box<dyn SchemaModifier>>;

    fn try_get_scalar_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Box<dyn GenericScalarFunction>>> {
        match self.try_get_entry(tx, name)? {
            Some(ent) => {
                let ent = ent.try_as_function()?;
                match ent.implementation {
                    FunctionImpl::Scalar(scalar) => Ok(Some(scalar)),
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    fn try_get_aggregate_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Box<dyn GenericAggregateFunction>>> {
        match self.try_get_entry(tx, name)? {
            Some(ent) => {
                let ent = ent.try_as_function()?;
                match &ent.implementation {
                    FunctionImpl::Aggregate(agg) => Ok(Some(agg.clone())),
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }
}

/// A schema implementation that holds all items in memory.
///
/// This may be intialized by reading a catalog from persistent storage, or
/// created on-demand.
#[derive(Debug, Default)]
pub struct InMemorySchema {
    // TODO: OIDs
    // TODO: Seperate maps for funcs/tables
    entries: HashMap<String, CatalogEntry>,
}

impl Schema for InMemorySchema {
    fn try_get_entry(&self, _tx: &CatalogTx, name: &str) -> Result<Option<CatalogEntry>> {
        Ok(self.entries.get(name).cloned())
    }

    fn get_data_table(&self, tx: &CatalogTx, ent: &TableEntry) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }

    fn get_modifier(&self, tx: &CatalogTx) -> Result<Box<dyn SchemaModifier>> {
        unimplemented!()
    }
}

impl InMemorySchema {
    fn create_scalar_function(
        &mut self,
        tx: &CatalogTx,
        create: CreateScalarFunctionInfo,
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
        create: CreateAggregateFunctionInfo,
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
