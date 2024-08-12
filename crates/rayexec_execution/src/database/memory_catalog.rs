use std::{collections::HashMap, sync::Arc};

use super::{
    catalog::CatalogTx,
    catalog_entry::{CatalogEntry, CatalogEntryInner, SchemaEntry, TableEntry},
    catalog_map::CatalogMap,
};
use rayexec_error::Result;
use scc::ebr::Guard;

// Using `scc` package for concurrent datastructures.
//
// Concurrency is needed since we're wrapping everything in arcs to allow lifetimes
// of entries to not rely on the database context (e.g. if we have entries on certain
// pipeline operators).
//
// `scc` has a neat property where hashmaps can be read without acquiring locks.
// This is beneficial for:
//
// - Having a single "memory" catalog implementation for real catalogs and
//   system catalogs. Function lookups don't need to acquire any locks. This is
//   good because we want to share a single read-only system catalog across all
//   sessions.
//
// However these methods require a Guard for EBR, but these don't actually
// matter for us. Any synchronization for data removal that's required for our
// use case will go through more typical transaction semantics with timestamps.
//
// I (Sean) opted for `scc` over DashMap primarily for the lock-free read-only
// properties. DashMap has a fixed number of shards, and any read will acquire a
// lock for that shard. `evmap` was an alternative with nice lock-free read
// properties, but `scc` seems more active.
//
// ---
//
// The memory catalog will be treated as an in-memory cache for external
// databases/catalogs with entries getting loaded in during binding. This lets
// us have consistent types for catalog/table access without requiring data
// sources implement that logic.

#[derive(Debug)]
pub struct MemoryCatalog {
    schemas: scc::HashIndex<String, Arc<MemorySchema>>,
}

impl MemoryCatalog {
    pub fn for_each_entry<F>(&self, tx: &CatalogTx, func: &mut F) -> Result<()>
    where
        F: FnMut(&String, &CatalogEntry) -> Result<()>,
    {
        let guard = Guard::new();
        for (name, schema) in self.schemas.iter(&guard) {
            func(name, &schema.schema)?;
            schema.for_each_entry(tx, func)?;
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
