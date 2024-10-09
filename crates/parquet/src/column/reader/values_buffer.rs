use super::{DataType, FixedLenPrimitiveValue, ParquetValueType};

pub trait ValuesBuffer<T: ParquetValueType> {
    /// Return the total number of items in the buffer, including slots that
    /// haven't been pushed to yet.
    fn total_len(&self) -> usize {
        unimplemented!()
    }

    /// Return the number of items that have been pushed to this buffer.
    ///
    /// Does not included slots that haven't been pushed to yet.
    fn filled_len(&self) -> usize {
        unimplemented!()
    }

    fn swap(&mut self, a: usize, b: usize) {
        unimplemented!()
    }

    unsafe fn fill_from_raw_bytes(&mut self, bytes: &[u8], num_values: usize)
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

    fn put_value(&mut self, idx: usize, val: &T) {
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
    unsafe fn fill_from_raw_bytes(&mut self, bytes: &[u8], num_values: usize)
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
