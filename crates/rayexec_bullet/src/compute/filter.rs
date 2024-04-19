use crate::array::{Array, BooleanArray, OffsetIndex, PrimitiveArray, VarlenArray, VarlenType};
use crate::storage::PrimitiveStorage;
use rayexec_error::{RayexecError, Result};

pub trait FilterKernel: Sized {
    /// Filter self using a selection array.
    fn filter(&self, selection: &BooleanArray) -> Result<Self>;
}

impl FilterKernel for Array {
    fn filter(&self, selection: &BooleanArray) -> Result<Self> {
        Ok(match self {
            Self::Float32(arr) => Array::Float32(arr.filter(selection)?),
            Self::Float64(arr) => Array::Float64(arr.filter(selection)?),
            Self::Int32(arr) => Array::Int32(arr.filter(selection)?),
            Self::Int64(arr) => Array::Int64(arr.filter(selection)?),
            Self::UInt32(arr) => Array::UInt32(arr.filter(selection)?),
            Self::UInt64(arr) => Array::UInt64(arr.filter(selection)?),
            Self::Utf8(arr) => Array::Utf8(arr.filter(selection)?),
            Self::LargeUtf8(arr) => Array::LargeUtf8(arr.filter(selection)?),
            Self::Binary(arr) => Array::Binary(arr.filter(selection)?),
            Self::LargeBinary(arr) => Array::LargeBinary(arr.filter(selection)?),
            _ => unimplemented!(), // TODO
        })
    }
}

impl<T: Copy> FilterKernel for PrimitiveArray<T> {
    fn filter(&self, selection: &BooleanArray) -> Result<Self> {
        if self.len() != selection.len() {
            return Err(RayexecError::new(
                "Selection array length doesn't equal array length",
            ));
        }

        // TODO: validity

        let values_iter = match self.values() {
            PrimitiveStorage::Vec(v) => v.iter(),
        };
        let select_iter = selection.values().iter();

        let iter = values_iter
            .zip(select_iter)
            .filter_map(|(v, take)| if take { Some(*v) } else { None });

        let arr = PrimitiveArray::from_iter(iter);

        Ok(arr)
    }
}

impl<T: VarlenType + ?Sized, O: OffsetIndex> FilterKernel for VarlenArray<T, O> {
    fn filter(&self, selection: &BooleanArray) -> Result<Self> {
        if self.len() != selection.len() {
            return Err(RayexecError::new(
                "Selection array length doesn't equal array length",
            ));
        }

        // TODO: Validity

        let values_iter = self.values_iter();
        let select_iter = selection.values().iter();

        let iter = values_iter
            .zip(select_iter)
            .filter_map(|(v, take)| if take { Some(v) } else { None });

        let arr = VarlenArray::from_iter(iter);

        Ok(arr)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn simple_filter_primitive() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let selection = BooleanArray::from_iter([true, false, true, false]);

        let filtered = arr.filter(&selection).unwrap();
        let expected = Int32Array::from_iter([6, 8]);
        assert_eq!(expected, filtered);
    }

    #[test]
    fn simple_filter_varlen() {
        let arr = Utf8Array::from_iter(["aaa", "bbb", "ccc", "ddd"]);
        let selection = BooleanArray::from_iter([true, false, true, false]);

        let filtered = arr.filter(&selection).unwrap();
        let expected = Utf8Array::from_iter(["aaa", "ccc"]);
        assert_eq!(expected, filtered);
    }
}
