use std::sync::Arc;

use futures::StreamExt;
use rayexec_error::{Result, ResultExt};
use rayexec_io::{FileLocation, FileProvider};
use serde_json::Deserializer;

use super::{action::Action, snapshot::Snapshot};

/// Relative path to delta log files.
const DELTA_LOG_PATH: &'static str = "_delta_log";

#[derive(Debug)]
pub struct Table {
    /// Root of the table.
    root: FileLocation,
    /// Provider for accessing files.
    provider: Arc<dyn FileProvider>,
    /// Snapshot of the table, including what files we have available to use for
    /// reading.
    snapshot: Snapshot,
}

impl Table {
    /// Try to load a table at the given location.
    pub async fn load(root: FileLocation, provider: Arc<dyn FileProvider>) -> Result<Self> {
        // TODO: Actually iterate through the commit log...

        let first = root.join([DELTA_LOG_PATH, "00000000000000000000.json"])?;

        let bytes = provider
            .file_source(first)?
            .read_stream()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // TODO: Either move this to a utility, or avoid doing it.
        let bytes = bytes.into_iter().fold(Vec::new(), |mut v, buf| {
            v.extend_from_slice(buf.as_ref());
            v
        });

        let actions = Deserializer::from_slice(&bytes)
            .into_iter::<Action>()
            .collect::<Result<Vec<_>, _>>()
            .context("failed to read first commit log")?;

        let snapshot = Snapshot::try_new_from_actions(actions)?;

        Ok(Table {
            root,
            provider,
            snapshot,
        })
    }
}
