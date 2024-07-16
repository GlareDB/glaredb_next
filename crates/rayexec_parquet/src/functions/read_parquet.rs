use futures::future::{BoxFuture, FutureExt};
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{
        check_named_args_is_empty, PlannedTableFunction, TableFunction, TableFunctionArgs,
    },
    runtime::ExecutionRuntime,
};
use rayexec_io::FileLocation;
use std::sync::Arc;

use crate::{metadata::Metadata, schema::convert_schema};

use super::datatable::RowGroupPartitionedDataTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadParquet;

impl TableFunction for ReadParquet {
    fn name(&self) -> &'static str {
        "read_parquet"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["parquet_scan"]
    }

    fn plan_and_initialize<'a>(
        &'a self,
        runtime: &'a Arc<dyn ExecutionRuntime>,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadParquetImpl::initialize(runtime.as_ref(), args))
    }
}

#[derive(Debug, Clone)]
pub struct ReadParquetImpl {
    location: FileLocation,
    metadata: Arc<Metadata>,
    schema: Schema,
}

impl ReadParquetImpl {
    async fn initialize(
        runtime: &dyn ExecutionRuntime,
        mut args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        check_named_args_is_empty(&ReadParquet, &args)?;
        if args.positional.len() != 1 {
            return Err(RayexecError::new("Expected one argument"));
        }

        // TODO: Glob, dispatch to object storage/http impls

        let location = args.positional.pop().unwrap().try_into_string()?;
        let location = FileLocation::parse(&location);

        let mut source = runtime.file_provider().file_source(location.clone())?;

        let size = source.size().await?;

        let metadata = Metadata::load_from(source.as_mut(), size).await?;
        let schema = convert_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(Self {
            location,
            metadata: Arc::new(metadata),
            schema,
        }))
    }
}

impl PlannedTableFunction for ReadParquetImpl {
    fn table_function(&self) -> &dyn TableFunction {
        &ReadParquet
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(RowGroupPartitionedDataTable {
            metadata: self.metadata.clone(),
            schema: self.schema.clone(),
            location: self.location.clone(),
            runtime: runtime.clone(),
        }))
    }
}
