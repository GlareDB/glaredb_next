use futures::TryStreamExt;
use rayexec_bullet::format::pretty::table::PrettyTable;
use rayexec_execution::engine::result::ExecutionResult;
use rayexec_execution::hybrid::client::{HybridClient, HybridConnectConfig};
use std::sync::Arc;

use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::datasource::DataSourceRegistry;
use rayexec_execution::engine::{session::Session, Engine};
use rayexec_execution::runtime::{PipelineExecutor, Runtime};
use tokio::sync::Mutex;

use crate::result_table::StreamingTable;

/// A wrapper around a session and an engine for when running the database in a
/// local, single user mode (e.g. in the CLI or through wasm).
#[derive(Debug)]
pub struct SingleUserEngine<P: PipelineExecutor, R: Runtime> {
    pub runtime: R,
    pub engine: Engine<P, R>,
    pub session: SingleUserSession<P, R>,
}

impl<P, R> SingleUserEngine<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    /// Create a new single user engine using the provided runtime and registry.
    pub fn try_new(executor: P, runtime: R, registry: DataSourceRegistry) -> Result<Self> {
        let engine = Engine::new_with_registry(executor, runtime.clone(), registry)?;
        let session = SingleUserSession {
            session: Arc::new(Mutex::new(engine.new_session()?)),
        };

        Ok(SingleUserEngine {
            runtime,
            engine,
            session,
        })
    }

    pub fn session(&self) -> &SingleUserSession<P, R> {
        &self.session
    }

    /// Execute a sql query, storing the results in a `ResultTable`.
    ///
    /// The provided sql string may include more than one query which will
    /// result in more than one table.
    pub async fn sql(&self, sql: &str) -> Result<Vec<ResultTable2>> {
        let results = {
            let mut session = self.session.session.lock().await;
            session.simple(sql).await?
        };

        let mut tables = Vec::with_capacity(results.len());
        for result in results {
            let batches: Vec<_> = result.stream.try_collect::<Vec<_>>().await?;
            tables.push(ResultTable2 {
                schema: result.output_schema,
                batches,
            });
        }

        Ok(tables)
    }

    /// Connect to a remote server for hybrid execution.
    pub async fn connect_hybrid(&self, connection_string: String) -> Result<()> {
        // TODO: Should this all be happening here, or move to session?
        //
        // Currently outside of the session to avoid have an additional async
        // method for the ping.

        let config = HybridConnectConfig::try_from_connection_string(&connection_string)?;
        let client = self.runtime.http_client();
        let hybrid = HybridClient::new(client, config);

        hybrid.ping().await?;
        self.session.session.lock().await.set_hybrid(hybrid);

        Ok(())
    }
}

/// Session connected to the above engine.
///
/// Cheaply cloneable.
#[derive(Debug, Clone)]
pub struct SingleUserSession<P: PipelineExecutor, R: Runtime> {
    /// The underlying session.
    ///
    /// Wrapped in a mutex since planning may alter session state. Needs a tokio
    /// mutex since the lock is held across an await (for async binding).
    ///
    /// The lock should not be held during actual execution (reading of the
    /// result stream).
    pub(crate) session: Arc<Mutex<Session<P, R>>>,
}

impl<P, R> SingleUserSession<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    /// Execute a single sql query.
    pub async fn query(&self, sql: &str) -> Result<StreamingTable> {
        let mut session = self.session.lock().await;

        let mut statements = session.parse(sql)?;
        let statement = match statements.len() {
            1 => statements.pop().unwrap(),
            other => {
                return Err(RayexecError::new(format!(
                    "Expected 1 statement, got {}",
                    other
                )))
            }
        };

        const UNNAMED: &str = "";

        session.prepare(UNNAMED, statement)?;
        session.bind(UNNAMED, UNNAMED).await?;

        let result = session.execute(UNNAMED).await?;

        Ok(StreamingTable { result })
    }

    pub async fn query2(&self, sql: &str) -> Result<ExecutionResult> {
        let mut session = self.session.lock().await;
        let mut results = session.simple(sql).await?;

        match results.len() {
            1 => Ok(results.pop().unwrap()),
            other => Err(RayexecError::new(format!(
                "Expected 1 query, got {}",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultTable2 {
    pub schema: Schema,
    pub batches: Vec<Batch>,
}

impl ResultTable2 {
    pub async fn collect_from_result_stream(result: ExecutionResult) -> Result<Self> {
        let batches: Vec<_> = result.stream.try_collect::<Vec<_>>().await?;
        Ok(ResultTable2 {
            schema: result.output_schema,
            batches,
        })
    }

    pub fn pretty_table(&self, width: usize, max_rows: Option<usize>) -> Result<PrettyTable> {
        PrettyTable::try_new(&self.schema, &self.batches, width, max_rows)
    }
}
