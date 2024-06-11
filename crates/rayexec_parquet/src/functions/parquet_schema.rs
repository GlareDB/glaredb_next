use rayexec_error::{RayexecError, Result};
use rayexec_execution::functions::table::{
    check_named_args_is_empty, GenericTableFunction, SpecializedTableFunction, TableFunctionArgs,
};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetSchema;

impl GenericTableFunction for ParquetSchema {
    fn name(&self) -> &'static str {
        "parquet_schema"
    }

    fn specialize(&self, mut args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        unimplemented!()
    }
}
