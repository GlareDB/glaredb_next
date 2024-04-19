use rayexec_error::{RayexecError, Result};

use crate::array::{Array, PrimitiveArray};

pub trait TakeKernel: Sized {
    /// Take values at the given indices to produce a new array.
    fn take(&self, indices: &[usize]) -> Result<Self>;
}

impl TakeKernel for Array {
    fn take(&self, indices: &[usize]) -> Result<Self> {
        Ok(match self {
            Self::Float32(arr) => Array::Float32(arr.take(indices)?),
            Self::Float64(arr) => Array::Float64(arr.take(indices)?),
            Self::Int32(arr) => Array::Int32(arr.take(indices)?),
            Self::Int64(arr) => Array::Int64(arr.take(indices)?),
            Self::UInt32(arr) => Array::UInt32(arr.take(indices)?),
            Self::UInt64(arr) => Array::UInt64(arr.take(indices)?),
            _ => unimplemented!(), // TODO
        })
    }
}

impl<T: Copy> TakeKernel for PrimitiveArray<T> {
    fn take(&self, indices: &[usize]) -> Result<Self> {
        if !indices.iter().all(|&idx| idx < self.len()) {
            return Err(RayexecError::new("Index out of bounds"));
        }

        let values = self.values();
        // TODO: validity
        let iter = indices.iter().map(|idx| *values.get(*idx).unwrap());
        let taken = Self::from_iter(iter);
        Ok(taken)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Int32Array;

    use super::*;

    #[test]
    fn simple_take_primitive() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let indices = [1, 1, 3, 0];
        let out = arr.take(&indices).unwrap();

        let expected = Int32Array::from_iter([7, 7, 9, 6]);
        assert_eq!(expected, out);
    }

    #[test]
    fn take_primitive_out_of_bounds() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let indices = [1, 1, 3, 4];

        let _ = arr.take(&indices).unwrap_err();
    }
}
