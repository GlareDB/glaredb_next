pub mod modify;
pub mod result_stream;
pub mod session;
pub mod vars;

use rayexec_error::Result;
use session::Session;
use std::sync::Arc;

use crate::{
    datasource::{DataSourceRegistry, MemoryDataSource},
    scheduler::Scheduler,
};

#[derive(Debug)]
pub struct Engine {
    scheduler: Scheduler,
    registry: Arc<DataSourceRegistry>,
}

impl Engine {
    pub fn try_new() -> Result<Self> {
        let registry =
            DataSourceRegistry::default().with_datasource("memory", Box::new(MemoryDataSource))?;
        Self::try_new_with_registry(registry)
    }

    pub fn try_new_with_registry(registry: DataSourceRegistry) -> Result<Self> {
        Ok(Engine {
            scheduler: Scheduler::try_new()?,
            registry: Arc::new(registry),
        })
    }

    pub fn new_session(&self) -> Result<Session> {
        Ok(Session::new(self.scheduler.clone(), self.registry.clone()))
    }
}
