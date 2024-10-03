use std::fmt::Debug;

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
pub struct TernaryExecutor;

impl TernaryExecutor {
    pub fn execute<'a, S1, S2, S3, B, Op>(
        array1: &'a Array,
        array2: &'a Array,
        array3: &'a Array,
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(
            <S1::Storage as AddressableStorage>::T,
            <S2::Storage as AddressableStorage>::T,
            <S3::Storage as AddressableStorage>::T,
            &mut OutputBuffer<B>,
        ),
        S1: PhysicalStorage<'a>,
        S2: PhysicalStorage<'a>,
        S3: PhysicalStorage<'a>,
        B: ArrayDataBuffer<'a>,
    {
        let len = validate_logical_len(&builder.buffer, array1)?;
        let _ = validate_logical_len(&builder.buffer, array2)?;
        let _ = validate_logical_len(&builder.buffer, array3)?;

        let validity = union_validities([array1.validity(), array2.validity(), array2.validity()])?;

        let selection1 = array1.selection_vector();
        let selection2 = array2.selection_vector();
        let selection3 = array3.selection_vector();
        let mut out_validity = None;

        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        match validity {
            Some(validity) => {
                let values1 = S1::get_storage(&array1.data)?;
                let values2 = S2::get_storage(&array2.data)?;
                let values3 = S3::get_storage(&array3.data)?;

                let mut out_validity_builder = Bitmap::new_with_all_true(len);

                for idx in 0..len {
                    if !validity.value_unchecked(idx) {
                        out_validity_builder.set_unchecked(idx, false);
                        continue;
                    }

                    let sel1 = selection::get_unchecked(selection1, idx);
                    let sel2 = selection::get_unchecked(selection2, idx);
                    let sel3 = selection::get_unchecked(selection3, idx);

                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };
                    let val3 = unsafe { values3.get_unchecked(sel3) };

                    output_buffer.idx = idx;
                    op(val1, val2, val3, &mut output_buffer);
                }

                out_validity = Some(out_validity_builder)
            }
            None => {
                let values1 = S1::get_storage(&array1.data)?;
                let values2 = S2::get_storage(&array2.data)?;
                let values3 = S3::get_storage(&array3.data)?;

                for idx in 0..len {
                    let sel1 = selection::get_unchecked(selection1, idx);
                    let sel2 = selection::get_unchecked(selection2, idx);
                    let sel3 = selection::get_unchecked(selection3, idx);

                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };
                    let val3 = unsafe { values3.get_unchecked(sel3) };

                    output_buffer.idx = idx;
                    op(val1, val2, val3, &mut output_buffer);
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

/// Execute an operation on three arrays.
#[derive(Debug, Clone, Copy)]
pub struct TernaryExecutor2;

impl TernaryExecutor2 {
    pub fn execute<Array1, Type1, Iter1, Array2, Type2, Iter2, Array3, Type3, Iter3, Output>(
        first: Array1,
        second: Array2,
        third: Array3,
        mut operation: impl FnMut(Type1, Type2, Type3) -> Output,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<Option<Bitmap>>
    where
        Output: Debug,
        Array1: ArrayAccessor<Type1, ValueIter = Iter1>,
        Array2: ArrayAccessor<Type2, ValueIter = Iter2>,
        Array3: ArrayAccessor<Type3, ValueIter = Iter3>,
        Iter1: Iterator<Item = Type1>,
        Iter2: Iterator<Item = Type2>,
        Iter3: Iterator<Item = Type3>,
    {
        if first.len() != second.len() || second.len() != third.len() {
            return Err(RayexecError::new(format!(
                "Differing lengths of arrays, got {}, {}, and {}",
                first.len(),
                second.len(),
                third.len(),
            )));
        }

        let validity = union_validities([first.validity(), second.validity(), third.validity()])?;

        match &validity {
            Some(validity) => {
                for ((first, (second, third)), valid) in first
                    .values_iter()
                    .zip(second.values_iter().zip(third.values_iter()))
                    .zip(validity.iter())
                {
                    if valid {
                        let out = operation(first, second, third);
                        buffer.push_value(out);
                    } else {
                        buffer.push_null();
                    }
                }
            }
            None => {
                for (first, (second, third)) in first
                    .values_iter()
                    .zip(second.values_iter().zip(third.values_iter()))
                {
                    let out = operation(first, second, third);
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
        executor::{
            builder::GermanVarlenBuffer,
            physical_type::{PhysicalI32, PhysicalUtf8},
        },
        scalar::ScalarValue,
    };

    #[test]
    fn ternary_substr() {
        let first = Array::from_iter(["alphabet", "horse", "cat"]);
        let second = Array::from_iter([3, 1, 2]);
        let third = Array::from_iter([2, 3, 1]);

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(3),
        };

        let got = TernaryExecutor::execute::<PhysicalUtf8, PhysicalI32, PhysicalI32, _, _>(
            &first,
            &second,
            &third,
            builder,
            |s: &str, from: i32, count: i32, buf: &mut OutputBuffer<_>| {
                let s = s
                    .chars()
                    .skip((from - 1) as usize) // To match postgres' 1-indexing
                    .take(count as usize)
                    .collect::<String>();
                buf.put(s.as_str())
            },
        )
        .unwrap();

        assert_eq!(ScalarValue::from("ph"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("hor"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("a"), got.physical_scalar(2).unwrap());
    }
}
