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
    runtime::{ExecutionRuntime, ExecutionScheduler},
};

#[derive(Debug)]
pub struct Engine<S: ExecutionScheduler> {
    runtime: Arc<dyn ExecutionRuntime>,
    registry: Arc<DataSourceRegistry>,
    system_catalog: SystemCatalog,
    scheduler: Arc<S>,
}

impl<S> Engine<S>
where
    S: ExecutionScheduler,
{
    pub fn new(runtime: Arc<dyn ExecutionRuntime>) -> Result<Self> {
        let registry =
            DataSourceRegistry::default().with_datasource("memory", Box::new(MemoryDataSource))?;
        Self::new_with_registry(runtime, registry)
    }

    pub fn new_with_registry(
        runtime: Arc<dyn ExecutionRuntime>,
        registry: DataSourceRegistry,
    ) -> Result<Self> {
        let system_catalog = SystemCatalog::new(&registry);

        unimplemented!()
        // Ok(Engine {
        //     runtime,
        //     registry: Arc::new(registry),
        //     system_catalog,
        // })
    }

    pub fn new_session(&self) -> Result<Session<S>> {
        let context = DatabaseContext::new(self.system_catalog.clone());
        Ok(Session::new(
            context,
            self.runtime.clone(),
            self.registry.clone(),
        ))
    }

    pub fn new_server_session(&self) -> Result<ServerSession> {
        let context = DatabaseContext::new(self.system_catalog.clone());
        Ok(ServerSession::new(
            context,
            self.runtime.clone(),
            self.registry.clone(),
        ))
    }

    pub fn runtime(&self) -> &dyn ExecutionRuntime {
        self.runtime.as_ref()
    }
}
