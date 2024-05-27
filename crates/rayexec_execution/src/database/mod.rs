use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::fmt::Debug;

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
}

pub trait Catalog: Debug + Sync + Send {
    fn get_schema(&self, tx: &CatalogTx, name: &str) -> Result<Option<&dyn Schema>>;

    fn create_table(&self, tx: &CatalogTx, schema: &str, create: CreateTable) -> Result<()> {
        let schema = self
            .get_schema(tx, schema)?
            .ok_or_else(|| RayexecError::new(format!("Missing schema : {schema}")))?;
        schema.create_table(tx, create)
    }
}

#[derive(Debug, Clone)]
pub struct CreateTable {
    name: String,
}

pub trait Schema: Debug + Sync + Send {
    fn get_entry(&self, tx: &CatalogTx, name: &str) -> Result<Option<&CatalogEntry>>;
    fn create_table(&self, tx: &CatalogTx, create: CreateTable) -> Result<()>;
}

pub enum CatalogEntry {
    Table(()),
    External(()),
}
