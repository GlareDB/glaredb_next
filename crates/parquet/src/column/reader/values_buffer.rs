use super::private::ParquetValueType;
use super::{DataType, FixedLenPrimitiveValue};

pub trait ValuesBuffer<T: ParquetValueType> {
    fn len(&self) -> usize {
        unimplemented!()
    }

    fn swap(&mut self, a: usize, b: usize) {
        unimplemented!()
    }

    fn as_slice(&self) -> &[T]
    where
        T: FixedLenPrimitiveValue,
    {
        unimplemented!()
    }

    fn as_slice_mut(&mut self) -> &mut [T]
    where
        T: FixedLenPrimitiveValue,
    {
        unimplemented!()
    }

    fn put_value(&mut self, idx: usize, val: &T) {
        unimplemented!()
    }
}

impl<T> ValuesBuffer<T> for Vec<T> where T: ParquetValueType {}
