use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{
        check_named_args_is_empty, PlannedTableFunction, SpecializedTableFunction, TableFunction,
        TableFunctionArgs,
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

    fn specialize(&self, mut args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        check_named_args_is_empty(self, &args)?;
        if args.positional.len() != 1 {
            return Err(RayexecError::new("Expected one argument"));
        }

        // TODO: Glob, dispatch to object storage/http impls

        let location = args.positional.pop().unwrap().try_into_string()?;
        let location = FileLocation::parse(&location);

        Ok(Box::new(ReadParquetImpl { location }))
    }
}

#[derive(Debug, Clone)]
pub struct ReadParquetImpl {
    pub(crate) location: FileLocation,
}

impl SpecializedTableFunction for ReadParquetImpl {
    fn name(&self) -> &'static str {
        "read_parquet_impl"
    }

    fn initialize(
        self: Box<Self>,
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(async move { self.initialize_inner(runtime.as_ref()).await })
    }
}

impl ReadParquetImpl {
    async fn initialize_inner(
        self,
        runtime: &dyn ExecutionRuntime,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let mut source = runtime.file_provider().file_source(self.location.clone())?;

        let size = source.size().await?;

        let metadata = Metadata::load_from(source.as_mut(), size).await?;
        let schema = convert_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(ReadParquetLocalRowGroupPartitioned {
            specialized: self,
            metadata: Arc::new(metadata),
            schema,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct ReadParquetLocalRowGroupPartitioned {
    specialized: ReadParquetImpl,
    metadata: Arc<Metadata>,
    schema: Schema,
}

impl PlannedTableFunction for ReadParquetLocalRowGroupPartitioned {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.specialized
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(RowGroupPartitionedDataTable {
            metadata: self.metadata.clone(),
            schema: self.schema.clone(),
            location: self.specialized.location.clone(),
            runtime: runtime.clone(),
        }))
    }
}
