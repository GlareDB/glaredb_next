use super::{BoundTableFunction, TableFunction, TableFunctionArgs};
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone, Copy)]
pub struct ReadParquet;

impl TableFunction for ReadParquet {
    fn name(&self) -> &str {
        "read_parquet"
    }

    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunction>> {
        unimplemented!()
    }
}

struct ReadParquetLocal {
    path: String, // For explain
}