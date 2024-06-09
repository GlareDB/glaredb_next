use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::{
    database::table::DataTable, engine::EngineRuntime, functions::table::SpecializedTableFunction,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReadParquetLocal {}

impl SpecializedTableFunction for ReadParquetLocal {
    fn schema<'a>(&'a mut self, runtime: &'a EngineRuntime) -> BoxFuture<Result<Schema>> {
        unimplemented!()
    }

    fn datatable(&mut self, runtime: &Arc<EngineRuntime>) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }
}
