use std::path::PathBuf;

use futures::future::BoxFuture;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    engine::EngineRuntime,
    functions::table::{
        check_named_args_is_empty, GenericTableFunction, InitializedTableFunction,
        SpecializedTableFunction, TableFunctionArgs,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadParquet;

impl GenericTableFunction for ReadParquet {
    fn name(&self) -> &'static str {
        "read_parquet"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["parquet_scan"]
    }

    fn specialize(&self, args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        unimplemented!()
    }
}
