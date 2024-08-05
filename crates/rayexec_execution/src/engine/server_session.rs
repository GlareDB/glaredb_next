use rayexec_error::Result;
use rayexec_parser::statement::Statement;
use serde::{de::DeserializeSeed, Deserializer, Serialize};

use crate::{
    database::DatabaseContext,
    datasource::DataSourceRegistry,
    hybrid::buffer::ServerStreamBuffers,
    logical::sql::binder::{bind_data::BindData, BoundStatement},
    runtime::{PipelineExecutor, QueryHandle, Runtime},
};
use std::sync::Arc;

/// A "server" session for doing remote planning and remote execution.
///
/// Keeps no state and very cheap to create. Essentially just encapsulates logic
/// for what should happen on the remote side for hybrid/distributed execution.
#[derive(Debug)]
pub struct ServerSession<P: PipelineExecutor, R: Runtime> {
    /// Context this session has access to.
    context: DatabaseContext,

    /// Registered data source implementations.
    registry: Arc<DataSourceRegistry>,

    /// Hybrid execution streams.
    streams: ServerStreamBuffers,

    executor: P,
    runtime: R,
}

impl<P, R> ServerSession<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    pub fn new(
        context: DatabaseContext,
        executor: P,
        runtime: R,
        registry: Arc<DataSourceRegistry>,
    ) -> Self {
        ServerSession {
            context,
            registry,
            streams: ServerStreamBuffers::default(),
            executor,
            runtime,
        }
    }

    /// Plans a partially bound query, preparing it for execution.
    ///
    /// An intermediate pipeline group will be returned. This is expected to be
    /// sent back to the client for execution.
    ///
    /// Failing to complete binding (e.g. unable to resolve a table) should
    /// result in an error. Otherwise we can assume that all references are
    /// bound and we can continue with planning for hybrid exec.
    pub async fn plan_partially_bound(
        &self,
        stmt: BoundStatement,
        bind_data: BindData,
    ) -> Result<(BoundStatement, BindData)> {
        // TODO: Check the statement and complete anything pending.
        // Straightforward.
        unimplemented!()
    }

    /// Plans a hyrbid query graph from a completely bound statement.
    pub fn plan_hybrid_graph(&self, stmt: BoundStatement, bind_data: BindData) -> Result<()> {
        // TODO: Statement -> logical with typical planning.
        //
        // Logical -> "stateless" pipeline. Will not be returning a query graph,
        // but pre-marked pipelines with locations where to execute.
        //
        // Handler should serialize "client" pipelines and send back to client.
        // "server" pipelines should immediately start executing.
        unimplemented!()
    }

    pub fn execute_pipelines(&self, pipelines: Vec<()>) {
        // TODO: Accept "stateless" pipelines. Inflate with states. Execute.
        //
        // Return something to allow remote cancelation (uuid).
        //
        // Probably change ExecutionRuntime to handle "state inflation" on
        // "stateless" pipelines.
        unimplemented!()
    }
}
