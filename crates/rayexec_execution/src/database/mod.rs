pub mod schema;

use rayexec_error::{RayexecError, Result};
use schema::{CreateTable, Schema};
use std::collections::HashMap;
use std::fmt::Debug;

use crate::functions::scalar::GenericScalarFunction;

#[derive(Debug)]
pub struct CatalogTx {}

impl CatalogTx {
    pub fn new() -> Self {
        CatalogTx {}
    }
}

#[derive(Debug)]
pub struct DatabaseContext {
    catalogs: HashMap<String, Box<dyn Catalog>>,
}

impl DatabaseContext {
    pub fn attach_catalog(
        &mut self,
        name: impl Into<String>,
        catalog: Box<dyn Catalog>,
    ) -> Result<()> {
        let name = name.into();
        if self.catalogs.contains_key(&name) {
            return Err(RayexecError::new(format!(
                "Catalog with name '{name}' already attached"
            )));
        }
        self.catalogs.insert(name, catalog);

        Ok(())
    }

    pub fn detach_catalog(&mut self, name: &str) -> Result<()> {
        if self.catalogs.remove(name).is_none() {
            return Err(RayexecError::new(format!(
                "Catalog with name '{name}' doesn't exist"
            )));
        }
        Ok(())
    }

    pub fn get_catalog(&self, name: &str) -> Option<&dyn Catalog> {
        self.catalogs.get(name).map(|c| c.as_ref())
    }
}

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

    fn get_scalar_function(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn GenericScalarFunction>>>;

    fn create_table(&mut self, tx: &CatalogTx, schema: &str, create: CreateTable) -> Result<()> {
        self.get_schema_mut(tx, schema)?.create_table(tx, create)
    }
}
