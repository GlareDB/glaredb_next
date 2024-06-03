use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::fmt::Debug;

use crate::database::catalog::Catalog;
use crate::database::storage::memory::MemoryCatalog;

pub trait DataSource: Sync + Send + Debug {
    /// Create a new catalog using the provided options.
    fn create_catalog(
        &self,
        options: HashMap<String, OwnedScalarValue>,
    ) -> Result<Box<dyn Catalog>>;
}

#[derive(Debug, Default)]
pub struct DataSourceRegistry {
    datasources: HashMap<String, Box<dyn DataSource>>,
}

impl DataSourceRegistry {
    pub fn with_datasource(
        mut self,
        name: impl Into<String>,
        datasource: Box<dyn DataSource>,
    ) -> Result<Self> {
        let name = name.into();
        if self.datasources.contains_key(&name) {
            return Err(RayexecError::new(format!(
                "Duplicate data source with name '{name}'"
            )));
        }
        self.datasources.insert(name, datasource);
        Ok(self)
    }

    pub fn get_datasource(&self, name: &str) -> Result<&dyn DataSource> {
        self.datasources
            .get(name)
            .map(|d| d.as_ref())
            .ok_or_else(|| RayexecError::new(format!("Missing data source: {name}")))
    }
}

#[derive(Debug)]
pub struct MemoryDataSource;

impl DataSource for MemoryDataSource {
    fn create_catalog(
        &self,
        options: HashMap<String, OwnedScalarValue>,
    ) -> Result<Box<dyn Catalog>> {
        if !options.is_empty() {
            return Err(RayexecError::new("Memory data source takes no options"));
        }

        Ok(Box::new(MemoryCatalog::new_with_schema("public")))
    }
}
