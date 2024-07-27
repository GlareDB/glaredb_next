use std::io::{ErrorKind, Read};

use rayexec_error::{RayexecError, Result, ResultExt};

use crate::batch::Batch;
use crate::field::Schema;
use crate::ipc::batch::ipc_to_batch;
use crate::ipc::schema::ipc_to_schema;

use super::gen::message;
use super::IpcConfig;

pub(crate) const CONTINUATION_MARKER: u32 = 0xFFFFFFFF;

#[derive(Debug)]
pub struct StreamReader<R: Read> {
    reader: R,
    buf: Vec<u8>,
    schema: Schema,
    conf: IpcConfig,
}

impl<R: Read> StreamReader<R> {
    /// Try to create a new stream reader.
    ///
    /// This will attempt to read the first message a schema. Every message
    /// afterwards can be assumed to be a batch with that schema.
    pub fn try_new(mut reader: R, conf: IpcConfig) -> Result<Self> {
        let mut buf = Vec::new();
        let did_read = read_encapsulated_header(&mut reader, &mut buf)?;
        if !did_read {
            return Err(RayexecError::new("Unexpected end of stream"));
        }

        let message = message::root_as_message(&buf[8..]).context("Failed to read flat buffer")?;
        let schema_ipc = match message.header_as_schema() {
            Some(ipc) => ipc,
            None => {
                return Err(RayexecError::new(format!(
                    "Unexpected header type: {:?}",
                    message.header_type()
                )))
            }
        };

        let schema = ipc_to_schema(schema_ipc, &conf)?;

        Ok(StreamReader {
            reader,
            buf,
            schema,
            conf,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn try_next_batch(&mut self) -> Result<Option<Batch>> {
        self.buf.clear();
        let did_read = read_encapsulated_header(&mut self.reader, &mut self.buf)?;
        if !did_read {
            return Ok(None);
        }

        let message =
            message::root_as_message(&self.buf[8..]).context("Failed to read flat buffer")?;
        let batch_ipc = match message.header_as_record_batch() {
            Some(ipc) => ipc,
            None => {
                // TODO: Dictionaries.
                return Err(RayexecError::new(format!(
                    "Unexpected header type: {:?}",
                    message.header_type()
                )));
            }
        };

        let batch = ipc_to_batch(batch_ipc, &self.buf, &self.schema, &self.conf)?;

        Ok(Some(batch))
    }
}

/// Reads an encapsulated message header.
///
/// May return Ok(false) if the stream is complete. A stream is complete if
/// either the stream returns and EOF, or writes a 0 size metatadata length.
fn read_encapsulated_header(reader: &mut impl Read, buf: &mut Vec<u8>) -> Result<bool> {
    buf.truncate(0);
    buf.resize(4, 0);

    match reader.read_exact(buf) {
        Ok(_) => (),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(false),
        Err(e) => return Err(e.into()),
    };

    if buf[0..4] != CONTINUATION_MARKER.to_le_bytes() {
        return Err(RayexecError::new(format!(
            "Unexpected bytes at beginning of reader: {buf:?}"
        )));
    }

    buf.resize(8, 0);
    let metadata_size = {
        reader.read_exact(&mut buf[4..8])?;
        u32::from_le_bytes(buf[4..8].try_into().unwrap())
    };

    if metadata_size == 0 {
        return Ok(false);
    }

    buf.resize(metadata_size as usize + 8, 0);
    reader.read_exact(&mut buf[8..])?;

    Ok(true)
}
