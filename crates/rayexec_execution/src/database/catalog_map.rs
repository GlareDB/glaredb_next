use dashmap::DashMap;
use rayexec_error::Result;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use super::{catalog::CatalogTx, catalog_entry::CatalogEntry};

/// Maps a name to some catalog entry.
#[derive(Debug)]
pub struct CatalogMap {
    entries: DashMap<String, Arc<CatalogEntry>>,
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
        unimplemented!()
    }
}
