use flatbuffers::FlatBufferBuilder;
use rayexec_error::{RayexecError, Result};
use std::io::Write;

use crate::{
    field::Schema,
    ipc::{
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
    buf: Vec<u8>,
    conf: IpcConfig,
}

impl<W: Write> StreamWriter<W> {
    pub fn try_new(mut writer: W, schema: Schema, conf: IpcConfig) -> Result<Self> {
        let buf = Vec::new();

        writer.write(&CONTINUATION_MARKER.to_be_bytes())?;

        let mut builder = FlatBufferBuilder::from_vec(buf);
        let schema_ipc = schema_to_ipc(&schema, &mut builder)?.as_union_value();

        let mut message = MessageBuilder::new(&mut builder);
        message.add_version(MetadataVersion::V5);
        message.add_header_type(MessageHeader::Schema);
        message.add_bodyLength(0);
        message.add_header(schema_ipc);
        let message = message.finish();

        builder.finish(message, None);
        let (buf, offset) = builder.collapse();

        writer.write_all(&buf[offset..])?;

        if &buf[offset..].len() % 8 != 0 {
            let idx = &buf[offset..].len() % 8;
            writer.write_all(&WRITE_PAD[idx..])?;
        }

        Ok(StreamWriter {
            writer,
            schema,
            buf,
            conf,
        })
    }
}

// fn write_encapsulated(writer: &mut impl Write, )
