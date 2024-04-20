use crate::array::{Array, OffsetIndex, PrimitiveArray, VarlenArray, VarlenType};
use crate::scalar::ScalarValue;
use crate::storage::PrimitiveStorage;
use rayexec_error::Result;
use std::ops::Add;

pub trait MinMaxKernel {
    type Output;

    fn min(&self) -> Option<Self::Output>;
    fn max(&self) -> Option<Self::Output>;
}

impl<T: Copy + Default + PartialOrd> MinMaxKernel for PrimitiveArray<T> {
    type Output = T;

    fn min(&self) -> Option<Self::Output> {
        primitive_reduce(self, |acc, &val| if acc < val { acc } else { val })
    }

    fn max(&self) -> Option<Self::Output> {
        primitive_reduce(self, |acc, &val| if acc > val { acc } else { val })
    }
}

// TODO: Traits probably not the way to go for this.
// impl<'a, T: VarlenType + PartialOrd, O: OffsetIndex> MinMaxKernel for &'a VarlenArray<T, O> {
//     type Output = &'a T;

//     fn min(&self) -> Option<Self::Output> {
//         varlen_reduce(self, |acc, val| if acc < val { acc } else { val })
//     }

//     fn max(&self) -> Option<Self::Output> {
//         varlen_reduce(self, |acc, val| if acc > val { acc } else { val })
//     }
// }

pub trait SumKernel {
    type Output;

    fn sum(&self) -> Option<Self::Output>;
    fn sum_checked(&self) -> Result<Option<Self::Output>>;
}

impl<T: Copy + Default + Add<Output = T>> SumKernel for PrimitiveArray<T> {
    type Output = T;

    fn sum(&self) -> Option<Self::Output> {
        primitive_reduce(self, |acc, &val| acc + val)
    }

    fn sum_checked(&self) -> Result<Option<Self::Output>> {
        unimplemented!()
    }
}

fn primitive_reduce<T: Default>(
    arr: &PrimitiveArray<T>,
    reduce_fn: impl Fn(T, &T) -> T,
) -> Option<T> {
    let values = match arr.values() {
        PrimitiveStorage::Vec(v) => v,
    };

    match &arr.validity().0 {
        Some(bitmap) => {
            if bitmap.popcnt() - arr.len() == 0 {
                // No "valid" values in array.
                return None;
            }

            let out = bitmap.index_iter().fold(T::default(), |acc, idx| {
                let value = values.get(idx).unwrap();
                reduce_fn(acc, value)
            });

            Some(out)
        }
        None => {
            let out = values
                .iter()
                .fold(T::default(), |acc, val| reduce_fn(acc, val));
            Some(out)
        }
    }
}

fn varlen_reduce<'a, T: VarlenType, O: OffsetIndex>(
    arr: &'a VarlenArray<T, O>,
    reduce_fn: impl Fn(&'a T, &'a T) -> &'a T,
) -> Option<&T> {
    match &arr.validity().0 {
        Some(bitmap) => {
            if bitmap.popcnt() - arr.len() == 0 {
                // No "valid" values in array.
                return None;
            }

            let out = bitmap
                .index_iter()
                .map(|idx| arr.value(idx).unwrap())
                .reduce(|acc, val| reduce_fn(acc, val));

            out
        }
        None => arr.values_iter().reduce(reduce_fn),
    }
}
