use dashmap::DashMap;
use rayexec_error::Result;
use scc::ebr::Guard;
use std::sync::Arc;

use parking_lot::RwLock;

use super::{catalog::CatalogTx, catalog_entry::CatalogEntry};

/// Maps a name to some catalog entry.
#[derive(Debug)]
pub struct CatalogMap {
    entries: scc::HashIndex<String, Arc<CatalogEntry>>,
}

impl CatalogMap {
    pub fn create_entry(&self, tx: &CatalogTx, name: String, entry: CatalogEntry) -> Result<()> {
        unimplemented!()
    }

    pub fn drop_entry(&self, tx: &CatalogTx, name: &str, cascade: bool) -> Result<()> {
        unimplemented!()
    }

    pub fn get_entry(&self, tx: &CatalogTx, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        unimplemented!()
    }

    pub fn for_each_entry<F>(&self, tx: &CatalogTx, func: &mut F) -> Result<()>
    where
        F: FnMut(&String, &CatalogEntry) -> Result<()>,
    {
        let guard = Guard::new();
        for (name, ent) in self.entries.iter(&guard) {
            func(name, ent.as_ref())?;
        }
        Ok(())
    }
}
