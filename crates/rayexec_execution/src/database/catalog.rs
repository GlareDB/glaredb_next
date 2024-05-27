use hashbrown::HashMap;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::sync::Arc;

use super::schema::{CreateTable, InMemorySchema, Schema};

#[derive(Debug)]
pub struct CatalogTx {}

impl CatalogTx {
    pub fn new() -> Self {
        CatalogTx {}
    }
}

/// Interface for accessing data.
///
/// It's expected that each data source implements its own version of the
/// catalog (and consequently a schema implementation). If a data source doens't
/// support a given operation (e.g. create schema for our bigquery data source),
/// an appropriate error should be returned.
pub trait Catalog: Debug + Sync + Send {
    fn try_get_schema(&self, tx: &CatalogTx, name: &str) -> Result<Option<&dyn Schema>>;
    fn try_get_schema_mut(&mut self, tx: &CatalogTx, name: &str)
        -> Result<Option<&mut dyn Schema>>;

    fn get_schema(&self, tx: &CatalogTx, name: &str) -> Result<&dyn Schema> {
        self.try_get_schema(tx, name)?
            .ok_or_else(|| RayexecError::new(format!("Missing schema '{name}'")))
    }

    fn get_schema_mut(&mut self, tx: &CatalogTx, name: &str) -> Result<&mut dyn Schema> {
        self.try_get_schema_mut(tx, name)?
            .ok_or_else(|| RayexecError::new(format!("Missing schema '{name}'")))
    }

    fn create_schema(&mut self, tx: &CatalogTx, name: &str) -> Result<()>;
    fn drop_schema(&mut self, tx: &CatalogTx, name: &str) -> Result<()>;

    fn create_table(&mut self, tx: &CatalogTx, schema: &str, create: CreateTable) -> Result<()> {
        self.get_schema_mut(tx, schema)?.create_table(tx, create)
    }
}

////Implementation of Catalog over a shared catalog (e.g. the global system
/// catalog that cannot be changed).
impl Catalog for &dyn Catalog {
    fn try_get_schema(&self, tx: &CatalogTx, name: &str) -> Result<Option<&dyn Schema>> {
        (*self).try_get_schema(tx, name)
    }

    fn try_get_schema_mut(
        &mut self,
        _tx: &CatalogTx,
        _name: &str,
    ) -> Result<Option<&mut dyn Schema>> {
        Err(RayexecError::new("Cannot get mutable schema"))
    }

    fn create_schema(&mut self, _tx: &CatalogTx, _name: &str) -> Result<()> {
        Err(RayexecError::new("Cannot create schema"))
    }

    fn drop_schema(&mut self, _tx: &CatalogTx, _name: &str) -> Result<()> {
        Err(RayexecError::new("Cannot drop schema"))
    }
}

/// In-memory implementation of a catalog.
///
////Can be intialized from reading a catalog from persistent storage, or created
/// on-demand.
#[derive(Debug)]
pub struct InMemoryCatalog {
    // TODO: OIDs
    schemas: HashMap<String, InMemorySchema>,
}

impl Catalog for InMemoryCatalog {
    fn try_get_schema(&self, _tx: &CatalogTx, name: &str) -> Result<Option<&dyn Schema>> {
        Ok(self.schemas.get(name).map(|s| s as _))
    }

    fn try_get_schema_mut(
        &mut self,
        _tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<&mut dyn Schema>> {
        Ok(self.schemas.get_mut(name).map(|s| s as _))
    }

    fn create_schema(&mut self, tx: &CatalogTx, name: &str) -> Result<()> {
        if self.schemas.contains_key(name) {
            return Err(RayexecError::new(format!("Schema '{name}' already exists")));
        }
        self.schemas.insert(name.to_string(), InMemorySchema::new());
        Ok(())
    }

    fn drop_schema(&mut self, tx: &CatalogTx, name: &str) -> Result<()> {
        unimplemented!()
    }
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        InMemoryCatalog {
            schemas: HashMap::new(),
        }
    }
}
