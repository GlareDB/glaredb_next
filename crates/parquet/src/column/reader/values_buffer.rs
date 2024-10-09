use super::{DataType, FixedLenPrimitiveValue, ParquetValueType, VarlenPrimitiveValue};

pub trait ValuesBuffer<T: ParquetValueType> {
    /// Remaining number of values this buffer can hold.
    fn remaining_len(&self) -> usize {
        unimplemented!()
    }

    fn swap(&mut self, a: usize, b: usize) {
        unimplemented!()
    }

    /// Push many additional values from the raw bytes representation.
    ///
    /// `num_values` is guaranteed to be less than or equal to `remaining_len`.
    ///
    /// `bytes` represents a slice of `T` holding `num_values` values. `bytes`
    /// has no alignment guarantees.
    unsafe fn push_many_from_raw_bytes(&mut self, bytes: &[u8], num_values: usize)
    where
        T: FixedLenPrimitiveValue,
    {
        unimplemented!()
    }

    // TODO: Remove, replace with fill_from_raw_bytes
    fn as_slice_mut(&mut self) -> &mut [T]
    where
        T: FixedLenPrimitiveValue,
    {
        unimplemented!()
    }

    fn reserve_varlen_capacity(&mut self, capacity: usize)
    where
        T: VarlenPrimitiveValue,
    {
        unimplemented!()
    }

    fn push_varlen_value(&mut self, val: &T)
    where
        T: VarlenPrimitiveValue,
    {
        unimplemented!()
    }
}

pub struct PrimitiveValuesBuffer<T: FixedLenPrimitiveValue> {
    fill_start: usize,
    values: Vec<T>,
}

impl<T> ValuesBuffer<T> for PrimitiveValuesBuffer<T>
where
    T: FixedLenPrimitiveValue,
{
    unsafe fn push_many_from_raw_bytes(&mut self, bytes: &[u8], num_values: usize)
    where
        T: FixedLenPrimitiveValue,
    {
        let fill_slice = &mut self.values[self.fill_start..];
        let raw_slice = &mut T::slice_as_bytes_mut(fill_slice)[..bytes.len()];
        raw_slice.copy_from_slice(bytes);

        self.fill_start += num_values;
    }
}

// impl<T> ValuesBuffer<T> for Vec<T> where T: FixedLenPrimitiveValue {
//     fn fill_from_raw_bytes(&mut self, bytes: &[u8], num_values: usize)
//     where
//         T: FixedLenPrimitiveValue, {
//             let raw
//         }
// }
