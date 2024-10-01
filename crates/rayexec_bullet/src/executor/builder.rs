use crate::{array::ArrayData, bitmap::Bitmap, datatype::DataType};

use super::physical_type::PhysicalType;

#[derive(Debug)]
pub struct ArrayBuilder<B> {
    pub(crate) datatype: DataType,
    pub(crate) validity: Option<Bitmap>,
    pub(crate) buffer: B,
}

pub trait ArrayDataBuffer<'a> {
    type State;
    type Type: PhysicalType<'a>;

    fn state(&self) -> Self::State;

    fn put(&mut self, idx: usize, val: Self::Type);

    fn into_data(self) -> ArrayData;
}
