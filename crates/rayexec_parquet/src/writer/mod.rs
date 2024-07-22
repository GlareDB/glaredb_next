use parquet::file::writer::{SerializedFileWriter, SerializedPageWriter};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use rayexec_io::FileSink;

#[derive(Debug)]
pub struct AsyncBatchWriter {
    writer: Box<dyn FileSink>,
    row_group_size: usize,
    serializer: SerializedFileWriter<Vec<u8>>,
}

impl AsyncBatchWriter {
    /// Encode and write a batch to the underlying file sink.
    pub async fn write(&mut self, batch: &Batch) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct InMemoryColumnChunk {}
