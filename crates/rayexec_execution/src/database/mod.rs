pub mod catalog;
pub mod catalog_entry;
pub mod create;
pub mod ddl;
pub mod drop;
pub mod entry;
pub mod memory_catalog;
pub mod storage;
pub mod system;
pub mod table;

mod catalog_map;

use catalog::Catalog;
use memory_catalog::MemoryCatalog;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::storage::catalog_storage::CatalogStorage;
use crate::storage::memory::MemoryTableStorage;
use crate::storage::table_storage::TableStorage;

#[derive(Debug, Clone)]
pub struct AttachInfo {
    /// Name of the data source this attached database is for.
    pub datasource: String,
    /// Options used for connecting to the database.
    ///
    /// This includes things like connection strings, and other possibly
    /// sensitive info.
    pub options: HashMap<String, OwnedScalarValue>,
}

#[derive(Debug, Clone)]
pub struct Database {
    pub catalog: Arc<MemoryCatalog>,
    pub catalog_storage: Option<Arc<dyn CatalogStorage>>,
    pub table_storage: Option<Arc<dyn TableStorage>>,
    pub attach_info: Option<AttachInfo>,
}

/// Root of all accessible catalogs.
#[derive(Debug)]
pub struct DatabaseContext {
    catalogs: HashMap<String, Arc<dyn Catalog>>,
    databases: HashMap<String, Database>,
}

impl DatabaseContext {
    /// Creates a new database context containing containing a builtin "system"
    /// catalog, and a "temp" catalog for temporary database items.
    ///
    /// By itself, this context cannot be used to persist data. Additional
    /// catalogs need to be attached via `attach_catalog`.
    pub fn new(system_catalog: Arc<MemoryCatalog>) -> Self {
        // TODO: Make system catalog actually read-only.
        let mut databases = HashMap::new();

        databases.insert(
            "system".to_string(),
            Database {
                catalog: system_catalog,
                catalog_storage: None,
                table_storage: None,
                attach_info: None,
            },
        );

        databases.insert(
            "temp".to_string(),
            Database {
                catalog: Arc::new(MemoryCatalog::default()),
                catalog_storage: None,
                table_storage: Some(Arc::new(MemoryTableStorage::default())),
                attach_info: None,
            },
        );

        DatabaseContext {
            catalogs: HashMap::new(),
            databases,
        }
    }

    pub fn system_catalog(&self) -> Result<&MemoryCatalog> {
        self.databases
            .get("system")
            .map(|d| d.catalog.as_ref())
            .ok_or_else(|| RayexecError::new("Missing system catalog"))
    }

    pub fn attach_database(&mut self, name: impl Into<String>, database: Database) -> Result<()> {
        let name = name.into();
        if self.catalogs.contains_key(&name) {
            return Err(RayexecError::new(format!(
                "Catalog with name '{name}' already attached"
            )));
        }
        self.databases.insert(name, database);

        Ok(())
    }

    pub fn detach_database(&mut self, name: &str) -> Result<()> {
        if self.databases.remove(name).is_none() {
            return Err(RayexecError::new(format!(
                "Database with name '{name}' doesn't exist"
            )));
        }
        Ok(())
    }

    pub fn database_exists(&self, name: &str) -> bool {
        self.databases.contains_key(name)
    }

    pub fn get_database(&self, name: &str) -> Result<&Database> {
        self.databases
            .get(name)
            .ok_or_else(|| RayexecError::new(format!("Missing catalog '{name}'")))
    }

    pub(crate) fn iter_catalogs(&self) -> impl Iterator<Item = (&String, &Arc<dyn Catalog>)> {
        self.catalogs.iter()
    }
}
