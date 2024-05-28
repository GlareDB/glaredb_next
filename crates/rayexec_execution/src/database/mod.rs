pub mod catalog;
pub mod create;
pub mod entry;
pub mod schema;
pub mod system;
pub mod table;

use catalog::{Catalog, CatalogTx};
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use system::SYSTEM_CATALOG;

use crate::functions::aggregate::GenericAggregateFunction;
use crate::functions::scalar::GenericScalarFunction;

/// Root of all accessible catalogs.
#[derive(Debug)]
pub struct DatabaseContext {
    catalogs: HashMap<String, Box<dyn Catalog>>,
}

impl DatabaseContext {
    /// Creates a new database context containing containing a builtin "system"
    /// catalog.
    ///
    /// By itself, this context cannot be used to store data. Additional
    /// catalogs need to be attached via `attach_catalog`.
    pub fn new() -> Self {
        let catalogs = [(
            "system".to_string(),
            Box::new(&*SYSTEM_CATALOG as &dyn Catalog) as _,
        )]
        .into_iter()
        .collect();

        DatabaseContext { catalogs }
    }

    pub fn system_catalog(&self) -> Result<&dyn Catalog> {
        self.catalogs
            .get("system")
            .map(|c| c.as_ref())
            .ok_or_else(|| RayexecError::new("Missing system catalog"))
    }

    pub fn get_builtin_scalar(&self, name: &str) -> Result<Option<Box<dyn GenericScalarFunction>>> {
        let tx = &CatalogTx::new();
        self.system_catalog()?
            .get_schema(tx, "glare_catalog")?
            .try_get_scalar_function(tx, name)
    }

    pub fn get_builtin_aggregate(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn GenericAggregateFunction>>> {
        let tx = &CatalogTx::new();
        self.system_catalog()?
            .get_schema(tx, "glare_catalog")?
            .try_get_aggregate_function(tx, name)
    }

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

    pub fn get_catalog(&self, name: &str) -> Result<&dyn Catalog> {
        self.catalogs
            .get(name)
            .map(|c| c.as_ref())
            .ok_or_else(|| RayexecError::new(format!("Missing catalog '{name}'")))
    }
}

impl Default for DatabaseContext {
    fn default() -> Self {
        Self::new()
    }
}
