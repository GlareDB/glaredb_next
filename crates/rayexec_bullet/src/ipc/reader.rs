use std::io::Read;

use rayexec_error::{RayexecError, Result, ResultExt};

use super::gen::message::{self, Message as IpcMessage, RecordBatch as IpcRecordBatch};
use super::gen::schema::Schema as IpcSchema;

pub(crate) const CONTINUATION_MARKER: u32 = 0xFFFFFFFF;

/// Reads individual encapsulated messages.
///
/// See: <https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format>
pub(crate) struct EncapsulatedReader<'a, R: Read> {
    reader: R,
    buf: &'a mut Vec<u8>,
    metadata_size: usize,
}

impl<'a, R: Read> EncapsulatedReader<'a, R> {
    pub fn try_new(mut reader: R, buf: &'a mut Vec<u8>) -> Result<Self> {
        let mut u32_buf = [0; 4];
        reader.read_exact(&mut u32_buf)?;
        if u32_buf != CONTINUATION_MARKER.to_be_bytes() {
            return Err(RayexecError::new(format!(
                "Unexpected bytes at beginning of reader: {buf:?}"
            )));
        }

        let metadata_size = {
            reader.read_exact(&mut u32_buf)?;
            u32::from_le_bytes(u32_buf)
        };

        buf.truncate(0);
        buf.resize(metadata_size as usize, 0);

        Ok(EncapsulatedReader {
            reader,
            buf,
            metadata_size: metadata_size as usize,
        })
    }

    pub fn read_ipc_schema(&mut self) -> Result<IpcSchema<'_>> {
        self.reader.read_exact(self.buf)?;
        let message = message::root_as_message(&self.buf).context("Failed to read flat buffer")?;
        message
            .header_as_schema()
            .ok_or_else(|| RayexecError::new("Message is not a schema"))
    }

    pub fn read_ipc_batch(&mut self) -> Result<IpcRecordBatch<'_>> {
        self.reader.read_exact(self.buf)?;
        let message = message::root_as_message(&self.buf).context("Failed to read flat buffer")?;
        message
            .header_as_record_batch()
            .ok_or_else(|| RayexecError::new("Message is not a record batch"))
    }
}
