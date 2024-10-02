use std::marker::PhantomData;
use std::sync::Arc;

use crate::{
    array::{ArrayData, BinaryData},
    bitmap::Bitmap,
    datatype::DataType,
    storage::{GermanVarlenStorage, PrimitiveStorage, INLINE_THRESHOLD},
};

use super::physical_type::{PhysicalType, VarlenType};

#[derive(Debug)]
pub struct ArrayBuilder<B> {
    pub(crate) datatype: DataType,
    pub(crate) buffer: B,
}

/// Trait for handling building up array data.
pub trait ArrayDataBuffer<'a> {
    type State;
    type Type;

    fn state(&self) -> Self::State;

    fn put(&mut self, idx: usize, val: Self::Type);

    fn into_data(self) -> ArrayData;
}

#[derive(Debug)]
pub struct PrimitiveBuffer<T> {
    pub(crate) values: Vec<T>,
}

impl<T> PrimitiveBuffer<T>
where
    T: Default + Copy,
    Vec<T>: Into<PrimitiveStorage<T>>,
{
    pub fn with_len(len: usize) -> Self {
        PrimitiveBuffer {
            values: vec![T::default(); len],
        }
    }
}

impl<'a, T> ArrayDataBuffer<'a> for PrimitiveBuffer<T>
where
    Vec<T>: Into<PrimitiveStorage<T>>,
    ArrayData: From<PrimitiveStorage<T>>,
{
    type State = ();
    type Type = T;

    fn state(&self) -> Self::State {
        ()
    }

    fn put(&mut self, idx: usize, val: Self::Type) {
        self.values[idx] = val
    }

    fn into_data(self) -> ArrayData {
        PrimitiveStorage::from(self.values).into()
    }
}

#[derive(Debug)]
pub struct GermanVarlenBuffer<T> {
    pub(crate) lens: Vec<i32>,
    pub(crate) inline_or_metadata: Vec<[u8; 12]>,
    pub(crate) data: Vec<u8>,
    pub(crate) _type: PhantomData<T>,
}

impl<T> GermanVarlenBuffer<T>
where
    T: VarlenType,
{
    pub fn with_len(len: usize) -> Self {
        GermanVarlenBuffer {
            lens: vec![0; len],
            inline_or_metadata: vec![[0; 12]; len],
            data: Vec::new(),
            _type: PhantomData,
        }
    }
}

impl<'a, T> ArrayDataBuffer<'a> for GermanVarlenBuffer<T>
where
    T: VarlenType + 'a,
{
    type State = ();
    type Type = &'a T;

    fn state(&self) -> Self::State {
        ()
    }

    fn put(&mut self, idx: usize, val: Self::Type) {
        let val = val.as_bytes();

        if val.len() as i32 <= INLINE_THRESHOLD {
            self.lens[idx] = val.len() as i32;

            let mut inline = [0; 12];
            inline[0..val.len()].copy_from_slice(val);
            self.inline_or_metadata[idx] = inline;
        } else {
            // Store prefix, buf index, and offset in line. Store complete copy
            // in buffer.

            self.lens[idx] = val.len() as i32;

            let mut metadata = [0; 12];

            // Prefix, 4 bytes
            let prefix_len = std::cmp::min(val.len(), 4);
            metadata[0..prefix_len].copy_from_slice(&val[0..prefix_len]);

            // Buffer index, currently always zero.

            // Offset, 4 bytes
            let offset = self.data.len();
            self.data.extend_from_slice(val);
            metadata[9..].copy_from_slice(&(offset as i32).to_le_bytes());

            self.inline_or_metadata.push(metadata);
        }
    }

    fn into_data(self) -> ArrayData {
        let storage = GermanVarlenStorage {
            lens: self.lens.into(),
            inline_or_metadata: self.inline_or_metadata.into(),
            data: self.data.into(),
        };

        ArrayData::Binary(BinaryData::German(Arc::new(storage)))
    }
}
