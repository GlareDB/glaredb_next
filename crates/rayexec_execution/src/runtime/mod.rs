pub mod dump;
pub mod hybrid;

use std::fmt::Debug;
use std::sync::Arc;

use dump::QueryDump;
use futures::future::BoxFuture;
use futures::Future;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use rayexec_io::http::{BoxedHttpResponse, HttpClient};
use rayexec_io::FileProvider;
use url::Url;

use crate::execution::pipeline::PartitionPipeline;
use crate::execution::query_graph::QueryGraph;
use crate::logical::sql::binder::StatementWithBindData;

pub trait ExecutionScheduler: Debug + Sync + Send {
    type ErrorSink: ErrorSink;
    type QueryHandle: QueryHandle;

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
        errors: Arc<Self::ErrorSink>,
    ) -> Self::QueryHandle;
}

/// An execution runtime handles driving execution for a query.
///
/// Implementations may make use of different strategies when executing a query.
// TODO: Split this up. Currently contains two separate concerns: dependencies
// required for data sources (tokio, http) and how to execute a query graph.
//
// This may also change to just return a reference to an "execution scheduler"
// which would handle the spawn, instead of having the spawn directly on this
// trait. This would allow changing out the execution part without needing to
// also change the "dependencies" part (which would be useful for a move to
// web-worker in wasm or distributed execution).
//
// See <https://github.com/GlareDB/rayexec/pull/99#discussion_r1664283835> for
// discussion.
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

    /// Returns a file provider that's able to construct file sources and sinks
    /// depending a location.
    fn file_provider(&self) -> Arc<dyn FileProvider>;

    fn hybrid_client(&self, conf: HybridConnectConfig) -> Arc<dyn HybridClient> {
        unimplemented!()
    }
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
