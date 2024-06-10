use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::{
    database::table::{DataTable, DataTableScan},
    engine::EngineRuntime,
    execution::operators::PollPull,
    functions::table::{InitializedTableFunction, SpecializedTableFunction},
};
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::Arc,
    task::Context,
};

use crate::{metadata::Metadata, schema::convert_schema};

#[derive(Debug, Clone)]
pub struct ReadParquetLocal {
    pub(crate) path: PathBuf,
}

impl SpecializedTableFunction for ReadParquetLocal {
    fn name(&self) -> &'static str {
        "read_parquet_local"
    }

    fn initialize(
        self: Box<Self>,
        _runtime: &EngineRuntime,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        Box::pin(async move { self.initialize_inner().await })
    }
}

impl ReadParquetLocal {
    async fn initialize_inner(self) -> Result<Box<dyn InitializedTableFunction>> {
        let file = self.open_file()?;
        let size = file
            .metadata()
            .context("failed to get file metadata")?
            .size();

        let metadata = Metadata::load_from(file, size as usize).await?;
        let schema = convert_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(ReadParquetLocalRowGroupPartitioned {
            specialized: self,
            metadata: Arc::new(metadata),
            schema,
        }))
    }

    fn open_file(&self) -> Result<File> {
        OpenOptions::new().read(true).open(&self.path).map_err(|e| {
            RayexecError::with_source(
                format!(
                    "Failed to open file at location: {}",
                    self.path.to_string_lossy()
                ),
                Box::new(e),
            )
        })
    }
}

#[derive(Debug, Clone)]
struct ReadParquetLocalRowGroupPartitioned {
    specialized: ReadParquetLocal,
    metadata: Arc<Metadata>,
    schema: Schema,
}

impl InitializedTableFunction for ReadParquetLocalRowGroupPartitioned {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.specialized
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn datatable(&self, _runtime: &Arc<EngineRuntime>) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }
}

/// Data table implementation which parallelizes on row groups. During scanning,
/// each returned scan object is responsible for distinct row groups to read.
#[derive(Debug)]
struct RowGroupPartitionedDataTable {}

impl DataTable for RowGroupPartitionedDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct RowGroupsScan {
    /// Independent file handle.
    file: File,

    /// Row groups this scan is responsible for.
    row_groups: VecDeque<usize>,
}

impl DataTableScan for RowGroupsScan {
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull> {
        unimplemented!()
    }
}
