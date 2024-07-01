use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{InitializedTableFunction, SpecializedTableFunction},
    runtime::ExecutionRuntime,
};
use rayexec_io::http::{HttpClient, HttpFileReader};
use std::sync::Arc;
use url::Url;

use crate::{metadata::Metadata, schema::convert_schema};

use super::datatable::{ReaderBuilder, RowGroupPartitionedDataTable};

#[derive(Debug, Clone)]
pub struct ReadParquetHttp {
    pub(crate) url: Url,
}

impl SpecializedTableFunction for ReadParquetHttp {
    fn name(&self) -> &'static str {
        "read_parquet_http"
    }

    fn initialize(
        self: Box<Self>,
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        Box::pin(async move { self.initialize_inner(runtime.as_ref()).await })
    }
}

impl ReadParquetHttp {
    async fn initialize_inner(
        self,
        runtime: &dyn ExecutionRuntime,
    ) -> Result<Box<dyn InitializedTableFunction>> {
        let tokio_handle = runtime.tokio_handle();

        // TODO: Make http client accept optional tokio handle.
        // TODO: This also conflicts with single threaded tokio + outer block on
        let mut reader = HttpClient::new(tokio_handle).reader(self.url.clone());
        let size = reader.content_length().await?;

        let metadata = Metadata::load_from(&mut reader, size).await?;
        let schema = convert_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(ReadParquetHttpRowGroupPartitioned {
            specialized: self,
            reader,
            metadata: Arc::new(metadata),
            schema,
        }))
    }
}

impl ReaderBuilder<HttpFileReader> for HttpFileReader {
    fn new_reader(&self) -> Result<HttpFileReader> {
        Ok(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct ReadParquetHttpRowGroupPartitioned {
    specialized: ReadParquetHttp,
    reader: HttpFileReader,
    metadata: Arc<Metadata>,
    schema: Schema,
}

impl InitializedTableFunction for ReadParquetHttpRowGroupPartitioned {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.specialized
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn datatable(&self, _runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(RowGroupPartitionedDataTable::new(
            self.reader.clone(),
            self.metadata.clone(),
            self.schema.clone(),
        )))
    }
}
