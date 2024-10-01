use crate::{array::ArrayData, bitmap::Bitmap, datatype::DataType, storage::PrimitiveStorage};

use super::physical_type::PhysicalType;

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
