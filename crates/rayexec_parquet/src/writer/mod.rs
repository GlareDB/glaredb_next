use parquet::{
    column::{
        page::{PageReader, PageWriter},
        writer::ColumnWriter,
    },
    file::writer::{SerializedFileWriter, SerializedPageWriter},
};
use rayexec_bullet::{array::Array, batch::Batch};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use rayexec_io::FileSink;
use std::{cell::RefCell, fmt, io, sync::Arc};

pub struct AsyncBatchWriter {
    /// Underlying sink.
    sink: Box<dyn FileSink>,
    /// Desired row group size.
    row_group_size: usize,
    /// In-memory writer.
    writer: SerializedFileWriter<Vec<u8>>,
    /// Current row group we're working on.
    current_row_group: RowGroupWriter,
}

impl AsyncBatchWriter {
    /// Encode and write a batch to the underlying file sink.
    pub async fn write(&mut self, batch: &Batch) -> Result<()> {
        unimplemented!()
    }

    fn write_buffered(&mut self, batch: &Batch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.current_row_group.write(batch)?;

        unimplemented!()
    }
}

impl fmt::Debug for AsyncBatchWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncBatchWriter").finish_non_exhaustive()
    }
}

struct RowGroupWriter {
    column_writers: Vec<ColumnWriter<SerializedPageWriter<'static, Vec<u8>>>>,
    /// Number of rows currently serialized in the row group.
    num_rows: usize,
}

impl RowGroupWriter {
    fn write(&mut self, batch: &Batch) -> Result<()> {
        for (writer, col) in self.column_writers.iter_mut().zip(batch.columns()) {
            write_array(writer, col)?;
        }

        self.num_rows += batch.num_rows();

        Ok(())
    }
}

fn array_type_err(arr: &Array) -> RayexecError {
    RayexecError::new(format!("Unexpected array type: {}", arr.datatype()))
}

/// Write an array into the column writer.
// TODO: Validity.
fn write_array<P: PageWriter>(
    writer: &mut ColumnWriter<P>,
    array: impl AsRef<Array>,
) -> Result<()> {
    let array = array.as_ref();
    match writer {
        ColumnWriter::BoolColumnWriter(writer) => match array {
            Array::Boolean(arr) => {
                // TODO: This could be `AsRef`ed
                let bools: Vec<_> = arr.values().iter().collect();
                writer
                    .write_batch(&bools, None, None)
                    .context("failed to write bools")?; // TODO: Def, rep
                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        ColumnWriter::Int64ColumnWriter(writer) => match array {
            Array::Int64(arr) => {
                writer
                    .write_batch(arr.values().as_ref(), None, None)
                    .context("failed to write int64s")?;
                Ok(())
            }
            Array::UInt64(arr) => {
                // Allow overflow.
                // TODO: AsRef instead of needing to collect.
                let vals: Vec<_> = arr.values().as_ref().iter().map(|v| *v as i64).collect();
                writer
                    .write_batch(&vals, None, None)
                    .context("failed to write uint64s")?;
                Ok(())
            }
            Array::Decimal64(arr) => {
                writer
                    .write_batch(arr.get_primitive().values().as_ref(), None, None)
                    .context("failed to write decimal64s")?;
                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        ColumnWriter::FloatColumnWriter(writer) => match array {
            Array::Float32(arr) => {
                writer
                    .write_batch(arr.values().as_ref(), None, None)
                    .context("failed to float32s")?;
                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        ColumnWriter::DoubleColumnWriter(writer) => match array {
            Array::Float64(arr) => {
                writer
                    .write_batch(arr.values().as_ref(), None, None)
                    .context("failed to write float64s")?;
                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        _other => not_implemented!("writer not implemented"),
    }
}
