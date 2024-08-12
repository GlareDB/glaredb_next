use std::{collections::HashMap, sync::Arc};

use super::{
    catalog::CatalogTx,
    catalog_entry::{CatalogEntry, CatalogEntryInner, SchemaEntry, TableEntry},
    catalog_map::CatalogMap,
};
use dashmap::DashMap;
use rayexec_error::Result;

#[derive(Debug)]
pub struct MemoryCatalog {
    schemas: DashMap<String, Arc<MemorySchema>>,
}

impl MemoryCatalog {
    pub fn for_each_entry<F>(&self, tx: &CatalogTx, func: &mut F) -> Result<()>
    where
        F: FnMut(&String, &CatalogEntry) -> Result<()>,
    {
        for schema_ref in self.schemas.iter() {
            func(schema_ref.key(), &schema_ref.value().schema)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MemorySchema {
    /// Catalog entry representing this schema.
    schema: CatalogEntry,
    /// All tables in the schema.
    tables: CatalogMap,
    /// All table functions in the schema.
    table_functions: CatalogMap,
    /// All scalar and aggregate functions in the schema.
    functions: CatalogMap,
}

impl MemorySchema {
    pub fn get_table(&self, tx: &CatalogTx, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        self.tables.get_entry(tx, name)
    }

    pub fn get_table_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        self.table_functions.get_entry(tx, name)
    }

    pub fn get_function(&self, tx: &CatalogTx, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        self.functions.get_entry(tx, name)
    }

    pub fn get_scalar_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let ent = self.functions.get_entry(tx, name)?;
        let ent = ent
            .map(|ent| match &ent.entry {
                CatalogEntryInner::ScalarFunction(_) => Some(ent),
                _ => None,
            })
            .flatten();

        Ok(ent)
    }

    pub fn get_aggregate_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let ent = self.functions.get_entry(tx, name)?;
        let ent = ent
            .map(|ent| match &ent.entry {
                CatalogEntryInner::AggregateFunction(_) => Some(ent),
                _ => None,
            })
            .flatten();

        Ok(ent)
    }

    pub fn for_each_entry<F>(&self, tx: &CatalogTx, func: &mut F) -> Result<()>
    where
        F: FnMut(&String, &CatalogEntry) -> Result<()>,
    {
        self.tables.for_each_entry(tx, func)?;
        self.table_functions.for_each_entry(tx, func)?;
        self.functions.for_each_entry(tx, func)?;
        Ok(())
    }
}
