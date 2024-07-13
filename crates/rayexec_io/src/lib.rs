pub mod filesystem;
pub mod http;

use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use rayexec_error::Result;
use std::fmt::Debug;

pub trait FileProvider<P>: Sync + Send + Debug {
    /// Gets a file source at some location.
    fn file_source(&self, location: P) -> Result<Box<dyn FileSource>>;

    /// Gets a file sink at some location
    fn file_sink(&self, location: P) -> Result<Box<dyn FileSink>>;
}

/// Asynchronous reads of some file source.
pub trait FileSource: Sync + Send + Debug {
    /// Read a complete range of bytes.
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>>;

    /// Stream bytes from a source.
    // TODO: Change to `into_read_stream`
    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>>;

    /// Get the size in bytes for a file.
    ///
    /// For data sources like parquet files, this is necessary as we need to be
    /// able to read the metadata at the end of a file to allow us to only fetch
    /// byte ranges.
    ///
    /// For other data sources like json and csv, this can be skipped and the
    /// content can just be streamed.
    fn size(&mut self) -> BoxFuture<Result<usize>>;
}

impl FileSource for Box<dyn FileSource + '_> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.as_mut().read_range(start, len)
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        self.as_mut().read_stream()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        self.as_mut().size()
    }
}

/// Asynchronous writes to some file source.
///
/// The semantics for this is overwrite any existing data. If appending is
/// needed, a separate trait should be created.
pub trait FileSink: Sync + Send + Debug {
    /// Write all bytes.
    fn write_all(&mut self, buf: &[u8]) -> BoxFuture<Result<()>>;

    /// Finish the write, including flushing out any pending bytes.
    ///
    /// This should be called after _all_ data has been written.
    fn finish(&mut self) -> BoxFuture<Result<()>>;
}
