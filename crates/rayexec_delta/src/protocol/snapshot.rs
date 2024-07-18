use std::collections::HashMap;

use rayexec_error::Result;
use tracing::trace;

use super::action::{Action, ActionAddFile, ActionChangeMetadata, ActionRemoveFile};

/// Snapshot of a table reconstructed from delta logs.
///
/// See <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#action-reconciliation>
#[derive(Debug)]
pub struct Snapshot {
    /// Latest metadata seen.
    metadata: ActionChangeMetadata,

    /// Add actions we've seen.
    add: HashMap<FileKey, ActionAddFile>,

    /// Remove actions we've seen.
    remove: HashMap<FileKey, ActionRemoveFile>,
}

impl Snapshot {
    /// Apply actions to produce an updated snapshot.
    pub fn apply_actions(&mut self, actions: impl IntoIterator<Item = Action>) -> Result<()> {
        for action in actions {
            trace!(?action, "reconciling action for snapshot");

            match action {
                Action::ChangeMetadata(metadata) => {
                    self.metadata = metadata;
                }
                Action::AddFile(add) => {
                    let key = FileKey {
                        path: add.path.clone(), // TODO: Avoid clone (probably just make path private and wrap in rc)
                        dv_id: None,            // TODO: Include deletion vector in action.
                    };

                    let _ = self.remove.remove(&key);
                    self.add.insert(key, add);
                }
                Action::RemoveFile(remove) => {
                    let key = FileKey {
                        path: remove.path.clone(),
                        dv_id: None,
                    };

                    let _ = self.add.remove(&key);
                    self.remove.insert(key, remove);
                }
                Action::AddCdcFile(_) => {
                    // Nothing to do.
                }
                Action::Transaction(_txn) => {
                    // TODO: Track latest tx version per app id.
                }
            }
        }

        Ok(())
    }
}

/// Key representing the "primary key" for a file.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileKey {
    /// Path to the file.
    path: String,
    /// Deletion vector ID if there is one associated for this key.
    dv_id: Option<String>,
}
