use flatbuffers::FlatBufferBuilder;
use rayexec_error::{RayexecError, Result};
use std::io::Write;

use crate::{
    batch::Batch,
    field::Schema,
    ipc::{
        batch::batch_to_header_ipc,
        gen::{
            message::{MessageBuilder, MessageHeader},
            schema::MetadataVersion,
        },
        reader::CONTINUATION_MARKER,
        schema::schema_to_ipc,
    },
};

use super::IpcConfig;

const WRITE_PAD: &[u8; 8] = &[0; 8];

#[derive(Debug)]
pub struct StreamWriter<W: Write> {
    writer: W,
    schema: Schema,
    data_buf: Vec<u8>,
    conf: IpcConfig,
}

impl<W: Write> StreamWriter<W> {
    pub fn try_new(mut writer: W, schema: Schema, conf: IpcConfig) -> Result<Self> {
        let mut builder = FlatBufferBuilder::new();
        let schema_ipc = schema_to_ipc(&schema, &mut builder)?.as_union_value();

        let mut message = MessageBuilder::new(&mut builder);
        message.add_version(MetadataVersion::V5);
        message.add_header_type(MessageHeader::Schema);
        message.add_bodyLength(0);
        message.add_header(schema_ipc);
        let message = message.finish();

        builder.finish(message, None);
        let buf = builder.finished_data();

        write_encapsulated_header(&mut writer, buf)?;

        writer.flush()?;

        Ok(StreamWriter {
            writer,
            schema,
            data_buf: Vec::new(),
            conf,
        })
    }

    pub fn write_batch(&mut self, batch: &Batch) -> Result<()> {
        let mut builder = FlatBufferBuilder::new();

        self.data_buf.clear();
        let batch_ipc =
            batch_to_header_ipc(batch, &mut self.data_buf, &mut builder)?.as_union_value();

        if self.data_buf.len() % 8 != 0 {
            self.data_buf
                .extend_from_slice(&WRITE_PAD[self.data_buf.len() % 8..]);
        }

        let mut message = MessageBuilder::new(&mut builder);
        message.add_version(MetadataVersion::V5);
        message.add_header_type(MessageHeader::Schema);
        message.add_bodyLength(self.data_buf.len() as i64);
        message.add_header(batch_ipc);
        let message = message.finish();

        builder.finish(message, None);
        let buf = builder.finished_data();

        write_encapsulated_header(&mut self.writer, buf)?;

        self.writer.write_all(&self.data_buf)?;

        self.writer.flush();

        Ok(())
    }
}

fn write_encapsulated_header(writer: &mut impl Write, buf: &[u8]) -> Result<()> {
    writer.write(&CONTINUATION_MARKER.to_be_bytes())?;

    let to_pad = 8 - (buf.len() % 8);
    let metadata_size = (buf.len() + to_pad) as u32;

    writer.write_all(&metadata_size.to_le_bytes())?;
    writer.write_all(buf)?;

    if to_pad > 0 {
        let idx = buf.len() % 8;
        writer.write_all(&WRITE_PAD[idx..])?;
    }

    Ok(())
}
