//! Conversion to/from ipc for batches.
use std::{collections::VecDeque, io::BufReader};

use crate::{
    array::{Array, PrimitiveArray},
    batch::Batch,
    bitmap::Bitmap,
    datatype::DataType,
    field::Schema,
    storage::PrimitiveStorage,
};

use super::{
    compression::CompressionType,
    gen::{
        message::{FieldNode as IpcFieldNode, RecordBatch as IpcRecordBatch},
        schema::Buffer,
    },
    IpcConfig,
};
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};

pub fn ipc_to_batch(
    batch: IpcRecordBatch,
    data: &[u8],
    schema: &Schema,
    conf: &IpcConfig,
) -> Result<Batch> {
    let mut buffers = BufferReader {
        data,
        decompress_buffer: Vec::new(),
        compression: None,
        buffers: batch.buffers().unwrap().iter().collect(),
        nodes: batch.nodes().unwrap().iter().collect(),
    };

    let mut columns = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let array = ipc_buffers_to_array(&mut buffers, &field.datatype)?;
        columns.push(array);
    }

    Batch::try_new(columns)
}

struct BufferReader<'a> {
    /// Complete message data.
    data: &'a [u8],

    /// Buffer for holding decompressed data.
    decompress_buffer: Vec<u8>,

    compression: Option<CompressionType>,

    /// "Buffers" from a record batch message. These only contain offsets and
    /// lengths, not the actual data.
    buffers: VecDeque<&'a Buffer>,

    nodes: VecDeque<&'a IpcFieldNode>,
}

impl<'a> BufferReader<'a> {
    fn try_next_buf(&mut self) -> Result<&'a [u8]> {
        let buf = self.buffers.pop_front().required("missing next buffer")?;

        match self.compression {
            Some(_) => {
                // TODO: Decompress into buffer, return that.
                not_implemented!("ipc decompression")
            }
            None => {
                let slice = &self.data[buf.offset() as usize..buf.length() as usize];
                Ok(slice)
            }
        }
    }

    fn try_next_node(&mut self) -> Result<&'a IpcFieldNode> {
        self.nodes.pop_front().required("missing next node")
    }
}

fn ipc_buffers_to_array(buffers: &mut BufferReader, datatype: &DataType) -> Result<Array> {
    match datatype {
        DataType::Int8 => Ok(Array::Int8(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::Int16 => Ok(Array::Int16(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::Int32 => Ok(Array::Int32(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::Int64 => Ok(Array::Int64(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::UInt8 => Ok(Array::UInt8(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::UInt16 => Ok(Array::UInt16(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::UInt32 => Ok(Array::UInt32(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::UInt64 => Ok(Array::UInt64(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        other => not_implemented!("ipc to array {other}"),
    }
}

fn ipc_buffers_to_primitive<T: Default + Copy>(
    node: &IpcFieldNode,
    buffers: [&[u8]; 2],
) -> Result<PrimitiveArray<T>> {
    let validity = if node.null_count() > 0 {
        let bitmap = Bitmap::try_new(buffers[0].to_vec(), node.length() as usize)?;
        Some(bitmap)
    } else {
        None
    };

    let values = PrimitiveStorage::<T>::copy_from_bytes(buffers[1])?;

    Ok(PrimitiveArray::new(values, validity))
}
