use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::functions::copy::{CopyToFunction, CopyToSink};
use rayexec_execution::runtime::ExecutionRuntime;
use rayexec_io::location::AccessConfig;
use rayexec_io::{location::FileLocation, FileSink};

use crate::reader::DialectOptions;
use crate::writer::CsvEncoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CsvCopyToFunction;

impl CopyToFunction for CsvCopyToFunction {
    fn name(&self) -> &'static str {
        "csv_copy_to"
    }

    // TODO: Access config
    fn create_sinks(
        &self,
        runtime: &Arc<dyn ExecutionRuntime>,
        schema: Schema,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn CopyToSink>>> {
        let provider = runtime.file_provider();

        let mut sinks = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            let sink = provider.file_sink(location.clone(), &AccessConfig::None)?;
            let dialect = DialectOptions::default();

            sinks.push(Box::new(CsvCopyToSink {
                encoder: CsvEncoder::new(schema.clone(), dialect),
                sink,
            }) as _)
        }

        Ok(sinks)
    }
}

#[derive(Debug)]
pub struct CsvCopyToSink {
    encoder: CsvEncoder,
    sink: Box<dyn FileSink>,
}

impl CsvCopyToSink {
    async fn push_inner(&mut self, batch: Batch) -> Result<()> {
        let mut buf = Vec::with_capacity(1024);
        self.encoder.encode(&batch, &mut buf)?;
        self.sink.write_all(buf.into()).await?;

        Ok(())
    }

    async fn finalize_inner(&mut self) -> Result<()> {
        self.sink.finish().await?;
        Ok(())
    }
}

impl CopyToSink for CsvCopyToSink {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        self.push_inner(batch).boxed()
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        self.finalize_inner().boxed()
    }
}
