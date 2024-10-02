use std::marker::PhantomData;
use std::sync::Arc;

use crate::{
    array::{ArrayData, BinaryData},
    datatype::DataType,
    storage::{
        GermanSmallMetadata, GermanVarlenStorage, PrimitiveStorage, UnionedGermanMetadata,
        INLINE_THRESHOLD,
    },
};

use super::physical_type::VarlenType;

#[derive(Debug)]
pub struct ArrayBuilder<B> {
    pub(crate) datatype: DataType,
    pub(crate) buffer: B,
}

#[derive(Debug)]
pub struct OutputBuffer<B> {
    pub(crate) idx: usize,
    pub(crate) buffer: B,
}

impl<'a, B> OutputBuffer<B>
where
    B: ArrayDataBuffer<'a>,
{
    pub fn put(&mut self, val: &B::Type) {
        self.buffer.put(self.idx, val)
    }
}

/// Trait for handling building up array data.
pub trait ArrayDataBuffer<'a> {
    type Type: ?Sized;

    fn put(&mut self, idx: usize, val: &Self::Type);

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
    T: Copy,
    Vec<T>: Into<PrimitiveStorage<T>>,
    ArrayData: From<PrimitiveStorage<T>>,
{
    type Type = T;

    fn put(&mut self, idx: usize, val: &Self::Type) {
        self.values[idx] = *val
    }

    fn into_data(self) -> ArrayData {
        PrimitiveStorage::from(self.values).into()
    }
}

#[derive(Debug)]
pub struct GermanVarlenBuffer<T: ?Sized> {
    pub(crate) metadata: Vec<UnionedGermanMetadata>,
    pub(crate) data: Vec<u8>,
    pub(crate) _type: PhantomData<T>,
}

impl<T> GermanVarlenBuffer<T>
where
    T: VarlenType + ?Sized,
{
    pub fn with_len(len: usize) -> Self {
        Self::with_len_and_data_capacity(len, 0)
    }

    pub fn with_len_and_data_capacity(len: usize, data_cap: usize) -> Self {
        GermanVarlenBuffer {
            metadata: vec![UnionedGermanMetadata::zero(); len],
            data: Vec::with_capacity(data_cap),
            _type: PhantomData,
        }
    }
}

impl<'a, T> ArrayDataBuffer<'a> for GermanVarlenBuffer<T>
where
    T: VarlenType + ?Sized,
{
    type Type = T;

    fn put(&mut self, idx: usize, val: &Self::Type) {
        let val = val.as_bytes();

        if val.len() as i32 <= INLINE_THRESHOLD {
            // Store completely inline.
            let meta = self.metadata[idx].as_small_mut();
            meta.len = val.len() as i32;
            meta.inline[0..val.len()].copy_from_slice(val);
        } else {
            // Store prefix, buf index, and offset in line. Store complete copy
            // in buffer.
            let meta = self.metadata[idx].as_large_mut();
            meta.len = val.len() as i32;

            // Prefix
            meta.prefix.copy_from_slice(&val[0..4]);

            // Buffer index, currently always zero.
            meta.buffer_idx = 0;

            // Offset, 4 bytes
            let offset = self.data.len();
            meta.offset = offset as i32;

            self.data.extend_from_slice(val);
        }
    }

    fn into_data(self) -> ArrayData {
        let storage = GermanVarlenStorage {
            metadata: self.metadata.into(),
            data: self.data.into(),
        };

        ArrayData::Binary(BinaryData::German(Arc::new(storage)))
    }
}
