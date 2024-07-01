pub mod dump;

use std::fmt::Debug;
use std::sync::Arc;

use dump::QueryDump;
use rayexec_error::{RayexecError, Result};
use rayexec_io::http::HttpClient;

use crate::execution::query_graph::QueryGraph;

/// An execution runtime handles driving execution for a query.
///
/// Implementations may make use of different strategies when executing a query.
pub trait ExecutionRuntime: Debug + Sync + Send {
    /// Spawn execution of a query graph.
    ///
    /// A query handle will be returned allowing for canceling and dumping a
    /// query.
    ///
    /// When execution encounters an unrecoverable error, the error will be
    /// written to the provided error sink. Recoverable errors should be handled
    /// internally.
    ///
    /// This must not block.
    fn spawn_query_graph(
        &self,
        query_graph: QueryGraph,
        errors: Arc<dyn ErrorSink>,
    ) -> Box<dyn QueryHandle>;

    /// Return a handle to a tokio runtime if this execution runtime has a tokio
    /// runtime configured.
    ///
    /// This is needed because our native execution runtime does not depend on
    /// tokio, but certain libraries and drivers that we want to use have an
    /// unavoidable dependency on tokio.
    ///
    /// Data sources should error if they require tokio and if this returns
    /// None.
    fn tokio_handle(&self) -> Option<tokio::runtime::Handle>;

    /// Get a new http client.
    ///
    /// May error if prereqs aren't met for creating an http client.
    fn http_client(&self) -> Result<Arc<dyn HttpClient>>;
}

pub trait QueryHandle: Debug + Sync + Send {
    /// Cancel the query.
    fn cancel(&self);

    /// Get a query dump.
    fn dump(&self) -> QueryDump;
}

pub trait ErrorSink: Debug + Sync + Send {
    /// Push an error.
    fn push_error(&self, error: RayexecError);
}
