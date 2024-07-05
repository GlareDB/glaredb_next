use futures::{StreamExt, TryStreamExt};
use std::sync::mpsc;
use std::{collections::VecDeque, sync::Arc};

use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::datasource::DataSourceRegistry;
use rayexec_execution::engine::{session::Session, Engine};
use rayexec_execution::runtime::ExecutionRuntime;
use tokio::sync::Mutex;

/// A wrapper around a session and an engine for when running the database in a
/// local, single user mode (e.g. in the CLI or through wasm).
#[derive(Debug)]
pub struct SingleUserEngine {
    pub engine: Engine,
    // TODO: Sean talks a lot about not needing a (tokio) mutex here, but adds
    // one anyway. This is the easiest solution for now. Sue me.
    pub session: Arc<Mutex<Session>>,
}

impl SingleUserEngine {
    /// Create a new single user engine using the provided runtime and registry.
    pub fn new_with_runtime(
        runtime: Arc<dyn ExecutionRuntime>,
        registry: DataSourceRegistry,
    ) -> Result<Self> {
        let engine = Engine::new_with_registry(runtime, registry)?;
        let session = engine.new_session()?;

        Ok(SingleUserEngine {
            engine,
            session: Arc::new(Mutex::new(session)),
        })
    }

    /// Execute a sql query, storing the results in a `ResultTable`.
    ///
    /// The provided sql string may include more than one query which will
    /// result in more than one table.
    pub async fn query(&self, sql: &str) -> Result<Vec<ResultTable>> {
        let results = {
            let mut session = self.session.lock().await;
            session.simple(sql).await?
        };

        let mut tables = Vec::with_capacity(results.len());
        for result in results {
            let batches: Vec<_> = result.stream.try_collect::<Vec<_>>().await?;
            tables.push(ResultTable {
                schema: result.output_schema,
                batches,
            });
        }

        Ok(tables)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultTable {
    pub schema: Schema,
    pub batches: Vec<Batch>,
}
