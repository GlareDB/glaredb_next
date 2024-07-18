use rayexec_error::Result;
use rayexec_io::{FileLocation, FileProvider};

use super::snapshot::Snapshot;

/// Relative path to delta log files.
const DELTA_LOG_PATH: &'static str = "_delta_log";

#[derive(Debug)]
pub struct Table {
    /// Root of the table.
    root: FileLocation,
    /// Provider for accessing files.
    provider: Box<dyn FileProvider>,
    /// Snapshot of the table, including what files we have available to use for
    /// reading.
    snapshot: Snapshot,
}

impl Table {
    /// Try to load a table at the given location.
    pub async fn load(root: FileLocation, provider: Box<dyn FileProvider>) -> Result<Self> {
        unimplemented!()
    }
}
