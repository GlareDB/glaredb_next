use rayexec_error::Result;
use std::{fmt::Debug, path::Path};

use crate::{FileSink, FileSource};

/// Provides access to a filesystem (real or virtual).
pub trait FileSystemProvider: Debug + Sync + Send + 'static {
    /// Get a read handle to some underlying file.
    fn reader(&self, path: &Path) -> Result<Box<dyn FileSource>>;

    /// Get a write handle to some underlying file.
    // TODO: Separate methods for "appender", and option to error if already exists.
    // TODO: Stronger semantics of what this means. This iteration is for COPY TO.
    fn sink(&self, path: &Path) -> Result<Box<dyn FileSink>>;
}
