use crate::{
    array::{Array, ArrayAccessor, ValuesBuffer},
    bitmap::Bitmap,
    executor::{
        builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer},
        physical_type::PhysicalStorage,
    },
    selection,
    storage::AddressableStorage,
};
use rayexec_error::Result;

use super::validate_logical_len;

#[derive(Debug, Clone)]
pub struct UnaryExecutor;

impl UnaryExecutor {
    pub fn execute<'a, S, B, Op>(
        array: &'a Array,
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(<S::Storage as AddressableStorage>::T, &mut OutputBuffer<B>),
        S: PhysicalStorage<'a>,
        B: ArrayDataBuffer<'a>,
    {
        let len = validate_logical_len(&builder.buffer, array)?;

        let selection = array.selection_vector();
        let mut out_validity = None;

        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        match &array.validity {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;
                let mut out_validity_builder = Bitmap::new_with_all_true(len);

                for idx in 0..len {
                    let sel = selection::get_unchecked(selection, idx);

                    if !validity.value_unchecked(idx) {
                        out_validity_builder.set_unchecked(idx, false);
                        continue;
                    }

                    let val = unsafe { values.get_unchecked(sel) };

                    output_buffer.idx = idx;
                    op(val, &mut output_buffer);
                }

                out_validity = Some(out_validity_builder)
            }
            None => {
                let values = S::get_storage(&array.data)?;
                for idx in 0..len {
                    let sel = selection::get_unchecked(selection, idx);
                    let val = unsafe { values.get_unchecked(sel) };

                    output_buffer.idx = idx;
                    op(val, &mut output_buffer);
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

/// Execute an operation on a single array.
#[derive(Debug, Clone, Copy)]
pub struct UnaryExecutor2;

impl UnaryExecutor2 {
    /// Execute an infallible operation on an array.
    pub fn execute<Array, Type, Iter, Output>(
        array: Array,
        mut operation: impl FnMut(Type) -> Output,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<()>
    where
        Array: ArrayAccessor<Type, ValueIter = Iter>,
        Iter: Iterator<Item = Type>,
    {
        match array.validity() {
            Some(validity) => {
                for (value, valid) in array.values_iter().zip(validity.iter()) {
                    if valid {
                        let out = operation(value);
                        buffer.push_value(out);
                    } else {
                        buffer.push_null();
                    }
                }
            }
            None => {
                for value in array.values_iter() {
                    let out = operation(value);
                    buffer.push_value(out);
                }
            }
        }

        Ok(())
    }

    /// Execute a potentially fallible operation on an array.
    pub fn try_execute<Array, Type, Iter, Output>(
        array: Array,
        mut operation: impl FnMut(Type) -> Result<Output>,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<()>
    where
        Array: ArrayAccessor<Type, ValueIter = Iter>,
        Iter: Iterator<Item = Type>,
    {
        match array.validity() {
            Some(validity) => {
                for (value, valid) in array.values_iter().zip(validity.iter()) {
                    if valid {
                        let out = operation(value)?;
                        buffer.push_value(out);
                    } else {
                        buffer.push_null();
                    }
                }
            }
            None => {
                for value in array.values_iter() {
                    let out = operation(value)?;
                    buffer.push_value(out);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use selection::SelectionVector;

    use crate::{
        datatype::DataType,
        executor::{
            builder::{GermanVarlenBuffer, PrimitiveBuffer},
            physical_type::{PhysicalI32, PhysicalUtf8},
        },
        scalar::ScalarValue,
    };

    use super::*;

    #[test]
    fn int32_inc_by_2() {
        let array = Array::from_iter([1, 2, 3]);
        let builder = ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        };

        let got = UnaryExecutor::execute::<PhysicalI32, _, _>(&array, builder, |v, buf| {
            buf.put(&(v + 2))
        })
        .unwrap();

        assert_eq!(ScalarValue::from(3), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from(4), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from(5), got.physical_scalar(2).unwrap());
    }

    #[test]
    fn string_double_named_func() {
        // Example with defined function, and allocating a new string every time.

        let array = Array::from_iter(["a", "bb", "ccc", "dddd"]);
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(4),
        };

        fn my_string_double<'a, B>(s: &str, buf: &mut OutputBuffer<B>)
        where
            B: ArrayDataBuffer<'a, Type = str>,
        {
            let mut double = s.to_string();
            double.push_str(s);
            buf.put(&double)
        }

        let got = UnaryExecutor::execute::<PhysicalUtf8, _, _>(&array, builder, my_string_double)
            .unwrap();

        assert_eq!(ScalarValue::from("aa"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("bbbb"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("cccccc"), got.physical_scalar(2).unwrap());
        assert_eq!(
            ScalarValue::from("dddddddd"),
            got.physical_scalar(3).unwrap()
        );
    }

    #[test]
    fn string_double_closure() {
        // Example with closure that reuses a string.

        let array = Array::from_iter(["a", "bb", "ccc", "dddd"]);
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(4),
        };

        let mut buffer = String::new();

        let my_string_double = |s: &str, buf: &mut OutputBuffer<_>| {
            buffer.clear();

            buffer.push_str(s);
            buffer.push_str(s);

            buf.put(buffer.as_str())
        };

        let got = UnaryExecutor::execute::<PhysicalUtf8, _, _>(&array, builder, my_string_double)
            .unwrap();

        assert_eq!(ScalarValue::from("aa"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("bbbb"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("cccccc"), got.physical_scalar(2).unwrap());
        assert_eq!(
            ScalarValue::from("dddddddd"),
            got.physical_scalar(3).unwrap()
        );
    }

    #[test]
    fn string_trunc_closure() {
        // Example with closure returning referencing to input.

        let array = Array::from_iter(["a", "bb", "ccc", "dddd"]);
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(4),
        };

        let my_string_truncate = |s: &str, buf: &mut OutputBuffer<_>| {
            let len = std::cmp::min(2, s.len());
            buf.put(s.get(0..len).unwrap_or(""))
        };

        let got = UnaryExecutor::execute::<PhysicalUtf8, _, _>(&array, builder, my_string_truncate)
            .unwrap();

        assert_eq!(ScalarValue::from("a"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("bb"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("cc"), got.physical_scalar(2).unwrap());
        assert_eq!(ScalarValue::from("dd"), got.physical_scalar(3).unwrap());
    }

    #[test]
    fn string_select_uppercase() {
        // Example with selection vector whose logical length is greater than
        // the underlying physical data len.

        let mut array = Array::from_iter(["a", "bb", "ccc", "dddd"]);
        let mut selection = SelectionVector::with_range(0..5);
        selection.set_unchecked(0, 3);
        selection.set_unchecked(1, 3);
        selection.set_unchecked(2, 3);
        selection.set_unchecked(3, 1);
        selection.set_unchecked(4, 2);
        array.select_mut(&selection.into());

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(array.logical_len()),
        };

        let my_string_uppercase = |s: &str, buf: &mut OutputBuffer<_>| {
            let s = s.to_uppercase();
            buf.put(s.as_str())
        };

        let got =
            UnaryExecutor::execute::<PhysicalUtf8, _, _>(&array, builder, my_string_uppercase)
                .unwrap();

        assert_eq!(ScalarValue::from("DDDD"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("DDDD"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("DDDD"), got.physical_scalar(2).unwrap());
        assert_eq!(ScalarValue::from("BB"), got.physical_scalar(3).unwrap());
        assert_eq!(ScalarValue::from("CCC"), got.physical_scalar(4).unwrap());
    }
}
