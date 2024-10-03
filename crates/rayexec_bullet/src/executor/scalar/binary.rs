use crate::{
    array::{validity::union_validities, Array, ArrayAccessor, ValuesBuffer},
    bitmap::Bitmap,
    executor::{
        builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer},
        physical_type::PhysicalStorage,
        scalar::validate_logical_len,
    },
    selection,
    storage::AddressableStorage,
};
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone, Copy)]
pub struct BinaryExecutor;

impl BinaryExecutor {
    pub fn execute<'a, S1, S2, B, Op>(
        array1: &'a Array,
        array2: &'a Array,
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(
            &<S1::Storage as AddressableStorage>::T,
            &<S2::Storage as AddressableStorage>::T,
            &mut OutputBuffer<B>,
        ),
        S1: PhysicalStorage<'a>,
        S2: PhysicalStorage<'a>,
        B: ArrayDataBuffer<'a>,
    {
        let len = validate_logical_len(&builder.buffer, array1)?;
        let _ = validate_logical_len(&builder.buffer, array2)?;

        let validity = union_validities([array1.validity(), array2.validity()])?;

        let selection1 = array1.selection_vector();
        let selection2 = array2.selection_vector();
        let mut out_validity = None;

        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        match validity {
            Some(validity) => {
                let values1 = S1::get_storage(&array1.data)?;
                let values2 = S2::get_storage(&array2.data)?;

                let mut out_validity_builder = Bitmap::new_with_all_true(len);

                for idx in 0..len {
                    if !validity.value_unchecked(idx) {
                        out_validity_builder.set_unchecked(idx, false);
                        continue;
                    }

                    let sel1 = selection::get_unchecked(selection1, idx);
                    let sel2 = selection::get_unchecked(selection2, idx);

                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };

                    output_buffer.idx = idx;
                    op(val1, val2, &mut output_buffer);
                }

                out_validity = Some(out_validity_builder)
            }
            None => {
                let values1 = S1::get_storage(&array1.data)?;
                let values2 = S2::get_storage(&array2.data)?;

                for idx in 0..len {
                    let sel1 = selection::get_unchecked(selection1, idx);
                    let sel2 = selection::get_unchecked(selection2, idx);

                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };

                    output_buffer.idx = idx;
                    op(val1, val2, &mut output_buffer);
                }
            }
        }

        let data = output_buffer.buffer.into_data();

        Ok(Array {
            datatype: builder.datatype,
            selection: None,
            validity: out_validity,
            data,
        })
    }
}

/// Execute an operation on two arrays.
#[derive(Debug, Clone, Copy)]
pub struct BinaryExecutor2;

impl BinaryExecutor2 {
    /// Executes a binary operator on two arrays.
    pub fn execute<Array1, Type1, Iter1, Array2, Type2, Iter2, Output>(
        left: Array1,
        right: Array2,
        mut operation: impl FnMut(Type1, Type2) -> Output,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<Option<Bitmap>>
    where
        Array1: ArrayAccessor<Type1, ValueIter = Iter1>,
        Array2: ArrayAccessor<Type2, ValueIter = Iter2>,
        Iter1: Iterator<Item = Type1>,
        Iter2: Iterator<Item = Type2>,
    {
        if left.len() != right.len() {
            return Err(RayexecError::new(format!(
                "Differing lengths of arrays, got {} and {}",
                left.len(),
                right.len()
            )));
        }

        let validity = union_validities([left.validity(), right.validity()])?;

        match &validity {
            Some(validity) => {
                for ((left_val, right_val), valid) in left
                    .values_iter()
                    .zip(right.values_iter())
                    .zip(validity.iter())
                {
                    if valid {
                        let out = operation(left_val, right_val);
                        buffer.push_value(out);
                    } else {
                        buffer.push_null();
                    }
                }
            }
            None => {
                for (left_val, right_val) in left.values_iter().zip(right.values_iter()) {
                    let out = operation(left_val, right_val);
                    buffer.push_value(out);
                }
            }
        }

        Ok(validity)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        datatype::DataType,
        executor::{
            builder::{GermanVarlenBuffer, PrimitiveBuffer},
            physical_type::{PhysicalI32, PhysicalStr},
        },
        scalar::ScalarValue,
    };

    use super::*;

    #[test]
    fn binary_simple_add() {
        let left = Array::from_iter([1, 2, 3]);
        let right = Array::from_iter([4, 5, 6]);

        let builder = ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        };

        let got = BinaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
            &left,
            &right,
            builder,
            |&a, &b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        assert_eq!(ScalarValue::from(5), got.value(0).unwrap());
        assert_eq!(ScalarValue::from(7), got.value(1).unwrap());
        assert_eq!(ScalarValue::from(9), got.value(2).unwrap());
    }

    #[test]
    fn binary_string_repeat() {
        let left = Array::from_iter([1, 2, 3]);
        let right = Array::from_iter(["hello", "world", "goodbye!"]);

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(3),
        };

        let mut string_buf = String::new();
        let got = BinaryExecutor::execute::<PhysicalI32, PhysicalStr, _, _>(
            &left,
            &right,
            builder,
            |&repeat, s, buf| {
                string_buf.clear();
                for _ in 0..repeat {
                    string_buf.push_str(s);
                }
                buf.put(string_buf.as_str())
            },
        )
        .unwrap();

        assert_eq!(ScalarValue::from("hello"), got.value(0).unwrap());
        assert_eq!(ScalarValue::from("worldworld"), got.value(1).unwrap());
        assert_eq!(
            ScalarValue::from("goodbye!goodbye!goodbye!"),
            got.value(2).unwrap()
        );
    }
}
