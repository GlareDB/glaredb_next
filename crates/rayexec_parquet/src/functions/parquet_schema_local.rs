use futures::future::BoxFuture;
use rayexec_error::Result;
use rayexec_execution::{
    engine::EngineRuntime,
    functions::table::{InitializedTableFunction, SpecializedTableFunction},
};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ParquetSchemaLocal {
    pub(crate) path: PathBuf,
}

impl SpecializedTableFunction for ParquetSchemaLocal {
    fn name(&self) -> &'static str {
        "parquet_schema_local"
    }

    fn initialize(
        self: Box<Self>,
        _runtime: &EngineRuntime,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        unimplemented!()
    }
}
