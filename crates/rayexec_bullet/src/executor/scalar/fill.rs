use std::borrow::Borrow;

use rayexec_error::Result;

use crate::{
    array::Array,
    bitmap::Bitmap,
    executor::{
        builder::{ArrayBuilder, ArrayDataBuffer},
        physical_type::PhysicalStorage,
    },
    selection,
    storage::AddressableStorage,
};

/// Singular mapping of a `from` index to a `to` index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillMapping {
    pub from: usize,
    pub to: usize,
}

impl From<(usize, usize)> for FillMapping {
    fn from(value: (usize, usize)) -> Self {
        FillMapping {
            from: value.0,
            to: value.1,
        }
    }
}

/// Incrementally put values into a new array buffer from existing arrays using
/// a fill map.
#[derive(Debug)]
pub struct FillState<B: ArrayDataBuffer> {
    validity: Bitmap,
    builder: ArrayBuilder<B>,
}

impl<B> FillState<B>
where
    B: ArrayDataBuffer,
{
    pub fn new(builder: ArrayBuilder<B>) -> Self {
        let validity = Bitmap::new_with_all_true(builder.buffer.len());
        FillState { validity, builder }
    }

    /// Fill a new array buffer using values from some other array.
    ///
    /// `fill_map` is an iterator of mappings that map indices from `array` to
    /// where they should be placed in the buffer.
    pub fn fill<'a, S, I>(&mut self, array: &'a Array, fill_map: I) -> Result<()>
    where
        S: PhysicalStorage<'a>,
        I: IntoIterator<Item = FillMapping>,
        <<S as PhysicalStorage<'a>>::Storage as AddressableStorage>::T:
            Borrow<<B as ArrayDataBuffer>::Type>,
    {
        let selection = array.selection_vector();

        match &array.validity {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                for mapping in fill_map.into_iter() {
                    let sel = selection::get_unchecked(selection, mapping.from);

                    if validity.value_unchecked(sel) {
                        let val = unsafe { values.get_unchecked(sel) };
                        self.builder.buffer.put(mapping.to, val.borrow());
                    } else {
                        self.validity.set_unchecked(mapping.to, false)
                    }
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;

                for mapping in fill_map.into_iter() {
                    let sel = selection::get_unchecked(selection, mapping.from);
                    let val = unsafe { values.get_unchecked(sel) };
                    self.builder.buffer.put(mapping.to, val.borrow());
                }
            }
        }

        Ok(())
    }

    pub fn finish(self) -> Array {
        let validity = if self.validity.is_all_true() {
            None
        } else {
            Some(self.validity)
        };

        Array {
            datatype: self.builder.datatype,
            selection: None,
            validity,
            data: self.builder.buffer.into_data(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        datatype::DataType,
        executor::{builder::PrimitiveBuffer, physical_type::PhysicalI32},
        scalar::ScalarValue,
    };

    use super::*;

    #[test]
    fn fill_simple_linear() {
        let mut state = FillState::new(ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        });

        let arr = Array::from_iter([4, 5, 6]);
        let mapping = [
            FillMapping { from: 0, to: 0 },
            FillMapping { from: 1, to: 1 },
            FillMapping { from: 2, to: 2 },
        ];

        state.fill::<PhysicalI32, _>(&arr, mapping).unwrap();

        let got = state.finish();

        assert_eq!(ScalarValue::from(4), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(6), got.logical_value(2).unwrap());
    }

    #[test]
    fn fill_out_of_order() {
        let mut state = FillState::new(ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        });

        let arr = Array::from_iter([4, 5, 6]);
        let mapping = [
            FillMapping { from: 0, to: 1 },
            FillMapping { from: 1, to: 2 },
            FillMapping { from: 2, to: 0 },
        ];

        state.fill::<PhysicalI32, _>(&arr, mapping).unwrap();

        let got = state.finish();

        assert_eq!(ScalarValue::from(6), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(4), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(2).unwrap());
    }

    #[test]
    fn fill_from_different_arrays() {
        let mut state = FillState::new(ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(6),
        });

        let arr1 = Array::from_iter([4, 5, 6]);
        let mapping1 = [
            FillMapping { from: 0, to: 2 },
            FillMapping { from: 1, to: 4 },
            FillMapping { from: 2, to: 0 },
        ];
        state.fill::<PhysicalI32, _>(&arr1, mapping1).unwrap();

        let arr2 = Array::from_iter([7, 8, 9]);
        let mapping2 = [
            FillMapping { from: 0, to: 1 },
            FillMapping { from: 1, to: 3 },
            FillMapping { from: 2, to: 5 },
        ];
        state.fill::<PhysicalI32, _>(&arr2, mapping2).unwrap();

        let got = state.finish();

        assert_eq!(ScalarValue::from(6), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(7), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(4), got.logical_value(2).unwrap());
        assert_eq!(ScalarValue::from(8), got.logical_value(3).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(4).unwrap());
        assert_eq!(ScalarValue::from(9), got.logical_value(5).unwrap());
    }
}
