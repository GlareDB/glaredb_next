use crate::{
    array::{
        validity::{self, union_validities},
        Array, ArrayAccessor, ValuesBuffer,
    },
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

use super::check_validity;

#[derive(Debug, Clone, Copy)]
pub struct UniformExecutor;

impl UniformExecutor {
    pub fn execute<'a, S, B, Op>(
        arrays: &[&'a Array],
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(&[<S::Storage as AddressableStorage>::T], &mut OutputBuffer<B>),
        S: PhysicalStorage<'a>,
        B: ArrayDataBuffer<'a>,
    {
        let len = match arrays.first() {
            Some(first) => validate_logical_len(&builder.buffer, first)?,
            None => return Err(RayexecError::new("Cannot execute on no arrays")),
        };

        for arr in arrays {
            let _ = validate_logical_len(&builder.buffer, arr)?;
        }

        let any_invalid = arrays.iter().any(|a| a.validity().is_some());

        let selections: Vec<_> = arrays.iter().map(|a| a.selection_vector()).collect();

        let mut out_validity = None;
        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        let mut op_inputs = Vec::with_capacity(arrays.len());

        if any_invalid {
            let storage_values: Vec<_> = arrays
                .iter()
                .map(|a| S::get_storage(&a.data))
                .collect::<Result<Vec<_>>>()?;

            let validities: Vec<_> = arrays.iter().map(|a| a.validity()).collect();

            let mut out_validity_builder = Bitmap::new_with_all_true(len);

            for idx in 0..len {
                op_inputs.clear();
                let mut row_invalid = false;
                for arr_idx in 0..arrays.len() {
                    let sel = selection::get_unchecked(selections[arr_idx], idx);
                    if row_invalid || !check_validity(sel, validities[arr_idx]) {
                        row_invalid = true;
                        out_validity_builder.set_unchecked(idx, false);
                        continue;
                    }

                    let val = unsafe { storage_values[arr_idx].get_unchecked(sel) };
                    op_inputs.push(val);
                }

                output_buffer.idx = idx;
                op(op_inputs.as_slice().try_into().unwrap(), &mut output_buffer);
            }

            out_validity = Some(out_validity_builder);
        } else {
            let storage_values: Vec<_> = arrays
                .iter()
                .map(|a| S::get_storage(&a.data))
                .collect::<Result<Vec<_>>>()?;

            for idx in 0..len {
                op_inputs.clear();
                for arr_idx in 0..arrays.len() {
                    let sel = selection::get_unchecked(selections[arr_idx], idx);
                    let val = unsafe { storage_values[arr_idx].get_unchecked(sel) };
                    op_inputs.push(val);
                }

                output_buffer.idx = idx;
                op(op_inputs.as_slice().try_into().unwrap(), &mut output_buffer);
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

/// Execute an operation on a uniform variadic number of arrays.
#[derive(Debug, Clone, Copy)]
pub struct UniformExecutor2;

impl UniformExecutor2 {
    pub fn execute<Array, Type, Iter, Output>(
        arrays: &[Array],
        mut operation: impl FnMut(&[Type]) -> Output,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<Option<Bitmap>>
    where
        Array: ArrayAccessor<Type, ValueIter = Iter>,
        Iter: Iterator<Item = Type>,
    {
        let len = match arrays.first() {
            Some(arr) => arr.len(),
            None => return Ok(None),
        };

        for arr in arrays {
            if arr.len() != len {
                return Err(RayexecError::new("Not all arrays are of the same length"));
            }
        }

        let validity = union_validities(arrays.iter().map(|arr| arr.validity()))?;

        // TODO: Length check

        let mut values_iters: Vec<_> = arrays.iter().map(|arr| arr.values_iter()).collect();

        let mut row_vals = Vec::with_capacity(arrays.len());

        match &validity {
            Some(validity) => {
                for valid in validity.iter() {
                    if valid {
                        row_vals.clear();

                        for iter in values_iters.iter_mut() {
                            let val = iter.next().expect("value to exist");
                            row_vals.push(val);
                        }

                        let out = operation(&row_vals);
                        buffer.push_value(out);
                    } else {
                        // When not valid, we still need to move through the
                        // underlying values iterators.
                        for iter in values_iters.iter_mut() {
                            let _ = iter.next().expect("value to exist");
                        }

                        buffer.push_null();
                    }
                }
            }
            None => {
                for _idx in 0..len {
                    row_vals.clear();

                    for iter in values_iters.iter_mut() {
                        let val = iter.next().expect("value to exist");
                        row_vals.push(val);
                    }

                    let out = operation(&row_vals);
                    buffer.push_value(out);
                }
            }
        }

        Ok(validity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        datatype::DataType,
        executor::{builder::GermanVarlenBuffer, physical_type::PhysicalUtf8},
        scalar::ScalarValue,
    };

    #[test]
    fn uniform_string_concat_row_wise() {
        let first = Array::from_iter(["a", "b", "c"]);
        let second = Array::from_iter(["1", "2", "3"]);
        let third = Array::from_iter(["dog", "cat", "horse"]);

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(3),
        };

        let mut string_buffer = String::new();

        let got = UniformExecutor::execute::<PhysicalUtf8, _, _>(
            &[&first, &second, &third],
            builder,
            |inputs, buf| {
                string_buffer.clear();
                for input in inputs {
                    string_buffer.push_str(input);
                }
                buf.put(string_buffer.as_str())
            },
        )
        .unwrap();

        assert_eq!(ScalarValue::from("a1dog"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("b2cat"), got.physical_scalar(1).unwrap());
        assert_eq!(
            ScalarValue::from("c3horse"),
            got.physical_scalar(2).unwrap()
        );
    }

    #[test]
    fn uniform_string_concat_row_wise_with_invalid() {
        let first = Array::from_iter(["a", "b", "c"]);
        let mut second = Array::from_iter(["1", "2", "3"]);
        second.set_physical_validity(1, false); // "2" => NULL
        let third = Array::from_iter(["dog", "cat", "horse"]);

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(3),
        };

        let mut string_buffer = String::new();

        let got = UniformExecutor::execute::<PhysicalUtf8, _, _>(
            &[&first, &second, &third],
            builder,
            |inputs, buf| {
                string_buffer.clear();
                for input in inputs {
                    string_buffer.push_str(input);
                }
                buf.put(string_buffer.as_str())
            },
        )
        .unwrap();

        assert_eq!(ScalarValue::from("a1dog"), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("c3horse"), got.logical_value(2).unwrap());
    }
}
