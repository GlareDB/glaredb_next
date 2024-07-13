use std::fmt::Debug;
use std::sync::Arc;

use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use rayexec_execution::{
    execution::query_graph::QueryGraph,
    runtime::{ErrorSink, ExecutionRuntime, QueryHandle},
};
use rayexec_io::{
    filesystem::FileSystemProvider,
    http::{HttpClient, ReqwestClient},
    FileLocation, FileProvider, FileSink, FileSource,
};

use crate::{
    filesystem::LocalFileSystemProvider,
    http::{WrappedReqwestClient, WrappedReqwestClientReader},
    threaded::ThreadedScheduler,
};

/// Inner behavior of the execution runtime.
// TODO: Single-threaded scheduler to run our SLTs on to ensure no operators
// block without making progress. Would not be used for anything else.
pub trait Scheduler: Sync + Send + Debug + Sized {
    type Handle: QueryHandle;

    fn try_new() -> Result<Self>;

    fn spawn_query_graph(
        &self,
        query_graph: QueryGraph,
        errors: Arc<dyn ErrorSink>,
    ) -> Self::Handle;
}

pub type ThreadedExecutionRuntime = NativeExecutionRuntime<ThreadedScheduler>;

/// Execution runtime that makes use of native threads and thread pools.
///
/// May optionally be configured with a tokio runtime _in addition_ to the
/// actual execution scheduler.
#[derive(Debug)]
pub struct NativeExecutionRuntime<S: Scheduler> {
    /// Scheduler for executing queries.
    scheduler: S,

    /// Optional tokio runtime that this execution runtime can be configured
    /// with.
    tokio: Option<Arc<tokio::runtime::Runtime>>,
}

impl<S: Scheduler> NativeExecutionRuntime<S> {
    pub fn try_new() -> Result<Self> {
        Ok(NativeExecutionRuntime {
            scheduler: S::try_new()?,
            tokio: None,
        })
    }

    pub fn with_tokio(mut self, tokio: Arc<tokio::runtime::Runtime>) -> Self {
        self.tokio = Some(tokio);
        self
    }

    pub fn with_default_tokio(mut self) -> Result<Self> {
        // TODO: I had to change this to multi threaded since there was a
        // deadlock with current_thread and a single worker. I _think_ this is
        // because in main we're using the tokio runtime + block_on.
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .thread_name("rayexec_tokio")
            .build()
            .context("Failed to build tokio runtime")?;
        self.tokio = Some(Arc::new(tokio));
        Ok(self)
    }
}

impl<S: Scheduler + 'static> ExecutionRuntime for NativeExecutionRuntime<S> {
    fn spawn_query_graph(
        &self,
        query_graph: QueryGraph,
        errors: Arc<dyn ErrorSink>,
    ) -> Box<dyn QueryHandle> {
        let handle = self.scheduler.spawn_query_graph(query_graph, errors);
        Box::new(handle) as _
    }

    fn tokio_handle(&self) -> Option<tokio::runtime::Handle> {
        self.tokio.as_ref().map(|rt| rt.handle().clone())
    }

    fn file_provider(&self) -> Arc<dyn FileProvider> {
        Arc::new(NativeFileProvider {
            handle: self.tokio_handle(),
        })
    }

    fn http_client(&self) -> Result<Arc<dyn HttpClient>> {
        match &self.tokio {
            Some(tokio) => Ok(Arc::new(WrappedReqwestClient {
                inner: ReqwestClient::default(),
                handle: tokio.handle().clone(),
            })),
            None => Err(RayexecError::new(
                "Cannot create http client, missing tokio runtime",
            )),
        }
    }

    fn filesystem(&self) -> Result<Arc<dyn FileSystemProvider>> {
        Ok(Arc::new(LocalFileSystemProvider))
    }
}

#[derive(Debug, Clone)]
pub struct NativeFileProvider {
    /// For http (reqwest).
    ///
    /// If we don't have it, we return an error when attempting to access an
    /// http file.
    handle: Option<tokio::runtime::Handle>,
}

impl FileProvider for NativeFileProvider {
    fn file_source(&self, location: FileLocation) -> Result<Box<dyn FileSource>> {
        match (location, self.handle.as_ref()) {
            (FileLocation::Url(url), Some(handle)) => Ok(Box::new(WrappedReqwestClientReader {
                inner: ReqwestClient::default().reader(url),
                handle: handle.clone(),
            })),
            (FileLocation::Url(_), None) => Err(RayexecError::new(
                "Cannot create http client, missing tokio runtime",
            )),
            (FileLocation::Path(path), _) => LocalFileSystemProvider.reader(&path),
        }
    }

    fn file_sink(&self, location: FileLocation) -> Result<Box<dyn FileSink>> {
        match (location, self.handle.as_ref()) {
            (FileLocation::Url(_url), _) => not_implemented!("http sink native"),
            (FileLocation::Path(path), _) => LocalFileSystemProvider.sink(&path),
        }
    }
}
