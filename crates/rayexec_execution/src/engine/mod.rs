pub mod result;
pub mod server_session;
pub mod session;
pub mod vars;

use rayexec_error::Result;
use server_session::ServerSession;
use session::Session;
use std::sync::Arc;

use crate::{
    database::{storage::system::SystemCatalog, DatabaseContext},
    datasource::{DataSourceRegistry, MemoryDataSource},
    runtime::{ExecutionScheduler, Runtime},
};

#[derive(Debug)]
pub struct Engine<S: ExecutionScheduler, R: Runtime> {
    registry: Arc<DataSourceRegistry>,
    system_catalog: SystemCatalog,
    scheduler: S,
    runtime: R,
}

impl<S, R> Engine<S, R>
where
    S: ExecutionScheduler,
    R: Runtime,
{
    pub fn new(scheduler: S, runtime: R) -> Result<Self> {
        let registry =
            DataSourceRegistry::default().with_datasource("memory", Box::new(MemoryDataSource))?;
        Self::new_with_registry(scheduler, runtime, registry)
    }

    pub fn new_with_registry(
        scheduler: S,
        runtime: R,
        registry: DataSourceRegistry,
    ) -> Result<Self> {
        let system_catalog = SystemCatalog::new(&registry);

        Ok(Engine {
            registry: Arc::new(registry),
            system_catalog,
            scheduler,
            runtime,
        })
    }

    pub fn new_session(&self) -> Result<Session<S, R>> {
        let context = DatabaseContext::new(self.system_catalog.clone());
        Ok(Session::new(
            context,
            self.scheduler.clone(),
            self.runtime.clone(),
            self.registry.clone(),
        ))
    }

    pub fn new_server_session(&self) -> Result<ServerSession<S, R>> {
        let context = DatabaseContext::new(self.system_catalog.clone());
        Ok(ServerSession::new(
            context,
            self.scheduler.clone(),
            self.runtime.clone(),
            self.registry.clone(),
        ))
    }
}
