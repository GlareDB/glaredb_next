use dashmap::DashMap;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_parser::statement::Statement;
use serde::{de::DeserializeSeed, Deserializer, Serialize};
use uuid::Uuid;

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    datasource::DataSourceRegistry,
    engine::vars::SessionVars,
    execution::intermediate::{
        planner::{IntermediateConfig, IntermediatePipelinePlanner},
        IntermediatePipeline, IntermediatePipelineGroup,
    },
    hybrid::buffer::ServerStreamBuffers,
    logical::sql::{
        binder::{bind_data::BindData, hybrid::HybridResolver, BoundStatement},
        planner::PlanContext,
    },
    optimizer::Optimizer,
    runtime::{PipelineExecutor, QueryHandle, Runtime},
};
use std::sync::Arc;

/// Output after planning a partially bound query, containing parts of the
/// pipeline the should be executed on the client.
#[derive(Debug)]
pub struct PartialPlannedPipeline {
    /// Id for the query.
    query_id: Uuid,
    /// Pipelines that should be executed on the client.
    pipelines: IntermediatePipelineGroup,
    /// Output schema for the query.
    schema: Schema,
}

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

    pending_pipelines: DashMap<Uuid, IntermediatePipeline>,

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
            pending_pipelines: DashMap::new(),
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
    ) -> Result<PartialPlannedPipeline> {
        let tx = CatalogTx::new();
        let resolver = HybridResolver::new(&tx, &self.context);
        let bind_data = resolver.resolve_all_unbound(bind_data).await?;

        // TODO: Remove session var requirement.
        let vars = SessionVars::new_local();

        let (mut logical, context) = PlanContext::new(&vars, &bind_data).plan_statement(stmt)?;

        let optimizer = Optimizer::new();
        logical.root = optimizer.optimize(logical.root)?;
        let schema = logical.schema()?;

        let planner = IntermediatePipelinePlanner::new(IntermediateConfig::default());
        // let pipelines = planner.plan_pipelines(logical.root, context, sink)

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
