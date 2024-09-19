use crate::{
    array::{validity::union_validities, ArrayAccessor, ValuesBuffer},
    bitmap::Bitmap,
};
use rayexec_error::{RayexecError, Result};

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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::{Utf8Array, VarlenArray, VarlenValuesBuffer};

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
