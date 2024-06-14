//! Scalar executors for generic vectorized execution over different types of
//! arrays.
//!
//! Structs may be extended to include a buffer in the future to avoid
//! operations having to allows strings or vecs when operating on string and
//! binary arrays.
//!
//! Explicit generic typing is used for unary, binary, and ternary operations as
//! those are likely to be the most common, so have these operations be
//! monomorphized is probably a good thing.

use crate::{
    array::{ArrayAccessor, ValuesBuffer},
    bitmap::Bitmap,
};
use rayexec_error::{RayexecError, Result};

/// Execute an operation on a single array.
#[derive(Debug, Clone, Copy)]
pub struct UnaryExecutor;

impl UnaryExecutor {
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

/// Execute an operation on two arrays.
#[derive(Debug, Clone, Copy)]
pub struct BinaryExecutor;

impl BinaryExecutor {
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

/// Execute an operation on three arrays.
#[derive(Debug, Clone, Copy)]
pub struct TernaryExecutor;

impl TernaryExecutor {
    pub fn execute<Array1, Type1, Iter1, Array2, Type2, Iter2, Array3, Type3, Iter3, Output>(
        first: Array1,
        second: Array2,
        third: Array3,
        mut operation: impl FnMut(Type1, Type2, Type3) -> Output,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<Option<Bitmap>>
    where
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

/// Execute an operation on a uniform variadic number of arrays.
#[derive(Debug, Clone, Copy)]
pub struct UniformExecutor;

impl UniformExecutor {
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

/// Union all validities.
///
/// The final bitmap will be the OR of all bitmaps.
pub fn union_validities<'a>(
    validities: impl IntoIterator<Item = Option<&'a Bitmap>>,
) -> Result<Option<Bitmap>> {
    let mut unioned: Option<Bitmap> = None;

    for bitmap in validities {
        match (&mut unioned, bitmap) {
            (Some(unioned), Some(bitmap)) => unioned.bit_or_mut(bitmap)?,
            (None, Some(bitmap)) => unioned = Some(bitmap.clone()),
            _ => (),
        }
    }

    Ok(unioned)
}

#[cfg(test)]
mod tests {
    use crate::array::{
        Int32Array, Int64Array, PrimitiveArray, Utf8Array, VarlenArray, VarlenValuesBuffer,
    };

    use super::*;

    #[test]
    fn binary_simple_add() {
        // Simple binary operation with differing input types.

        let left = Int32Array::from_iter([1, 2, 3]);
        let right = Int64Array::from_iter([4, 5, 6]);

        let mut buffer = Vec::with_capacity(3);

        let op = |a, b| (a as i64) + b;

        let validity = BinaryExecutor::execute(&left, &right, op, &mut buffer).unwrap();

        let got = PrimitiveArray::new(buffer, validity);
        let expected = Int64Array::from_iter([5, 7, 9]);

        assert_eq!(expected, got);
    }

    #[test]
    fn binary_string_repeat() {
        let left = Int32Array::from_iter([1, 2, 3]);
        let right = Utf8Array::from_iter(["hello", "world", "goodbye!"]);

        let mut buffer = VarlenValuesBuffer::default();

        let op = |a: i32, b: &str| b.repeat(a as usize);

        let validity = BinaryExecutor::execute(&left, &right, op, &mut buffer).unwrap();

        let got = VarlenArray::new(buffer, validity);
        let expected = Utf8Array::from_iter(["hello", "worldworld", "goodbye!goodbye!goodbye!"]);

        assert_eq!(expected, got);
    }

    #[test]
    fn ternary_substr() {
        let first = Utf8Array::from_iter(["alphabet"]);
        let second = Int32Array::from_iter([3]);
        let third = Int32Array::from_iter([2]);

        let mut buffer = VarlenValuesBuffer::default();

        let op = |s: &str, from: i32, count: i32| {
            s.chars()
                .skip((from - 1) as usize) // To match postgres' 1-indexing
                .take(count as usize)
                .collect::<String>()
        };

        let validity = TernaryExecutor::execute(&first, &second, &third, op, &mut buffer).unwrap();

        let got = VarlenArray::new(buffer, validity);
        let expected = Utf8Array::from_iter(["ph"]);

        assert_eq!(expected, got);
    }

    #[test]
    fn uniform_string_concat_row_wise() {
        let first = Utf8Array::from_iter(["a", "b", "c"]);
        let second = Utf8Array::from_iter(["1", "2", "3"]);
        let third = Utf8Array::from_iter(["dog", "cat", "horse"]);

        let mut buffer = VarlenValuesBuffer::default();

        let op = |strings: &[&str]| strings.join("");

        let validity =
            UniformExecutor::execute(&[&first, &second, &third], op, &mut buffer).unwrap();

        let got = VarlenArray::new(buffer, validity);
        let expected = Utf8Array::from_iter(["a1dog", "b2cat", "c3horse"]);

        assert_eq!(expected, got);
    }
}
