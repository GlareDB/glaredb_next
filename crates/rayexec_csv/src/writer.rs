use csv::ByteRecord;
use rayexec_bullet::{
    batch::Batch,
    field::Schema,
    format::{FormatOptions, Formatter},
};
use rayexec_error::{Result, ResultExt};
use rayexec_io::FileSink;
use std::io::Write as _;

use crate::reader::DialectOptions;

#[derive(Debug)]
pub struct AsyncCsvWriter<S: FileSink> {
    /// File we're writing to.
    sink: S,

    /// Schema of the batches we're writing. Used to write the header out.
    schema: Schema,

    /// If we've already written the header.
    did_write_header: bool,

    /// Dialect of csv we're writing.
    dialect: DialectOptions,

    /// Buffer used for formatting the batch.
    format_buf: Vec<u8>,

    /// Buffer used for storing the output.
    output_buf: Vec<u8>,

    /// Buffer for current record.
    record: ByteRecord,
}

impl<S: FileSink> AsyncCsvWriter<S> {
    pub fn new(sink: S, schema: Schema, dialect: DialectOptions) -> Self {
        let record = ByteRecord::with_capacity(1024, schema.fields.len());
        AsyncCsvWriter {
            sink,
            schema,
            dialect,
            did_write_header: false,
            format_buf: Vec::with_capacity(1024),
            output_buf: Vec::with_capacity(1024),
            record,
        }
    }

    pub async fn write(&mut self, batch: &Batch) -> Result<()> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions::new());

        self.output_buf.clear();
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.dialect.delimiter)
            .quote(self.dialect.quote)
            .from_writer(&mut self.output_buf);

        if !self.did_write_header {
            for col_name in self.schema.fields.iter().map(|f| &f.name) {
                self.record.push_field(col_name.as_bytes());
            }
            csv_writer
                .write_record(&self.record)
                .context("failed to write header")?;

            self.did_write_header = true;
        }

        for row in 0..batch.num_rows() {
            self.record.clear();

            for col in batch.columns() {
                let scalar = FORMATTER
                    .format_array_value(col, row)
                    .expect("row to exist");
                self.format_buf.clear();
                write!(&mut self.format_buf, "{}", scalar).expect("write to succeed");

                self.record.push_field(&self.format_buf);
            }

            csv_writer
                .write_record(&self.record)
                .context("failed to write record")?;
        }

        csv_writer.flush().context("failed to flush")?;
        std::mem::drop(csv_writer);

        self.sink.write_all(&self.output_buf).await?;

        Ok(())
    }

    pub fn into_inner(self) -> S {
        self.sink
    }
}
