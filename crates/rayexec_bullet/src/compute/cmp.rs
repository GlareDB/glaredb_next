use crate::{
    array::{Array, BooleanArray, PrimitiveArray},
    bitmap::Bitmap,
    scalar::ScalarValue,
    storage::PrimitiveStorage,
};
use rayexec_error::{RayexecError, Result};

pub trait CmpKernel<Rhs = Self> {
    type Output;

    fn eq(&self, right: &Rhs) -> Result<Self::Output>;
    fn neq(&self, right: &Rhs) -> Result<Self::Output>;
    fn lt(&self, right: &Rhs) -> Result<Self::Output>;
    fn lt_eq(&self, right: &Rhs) -> Result<Self::Output>;
    fn gt(&self, right: &Rhs) -> Result<Self::Output>;
    fn gt_eq(&self, right: &Rhs) -> Result<Self::Output>;
}

// TODO: Varlens
macro_rules! array_cmp_dispatch {
    ($left:ident, $right:ident, $fn:expr) => {{
        match ($left, $right) {
            (Array::Float32(left), Array::Float32(right)) => $fn(left, right),
            (Array::Float64(left), Array::Float64(right)) => $fn(left, right),
            (Array::Int32(left), Array::Int32(right)) => $fn(left, right),
            (Array::Int64(left), Array::Int64(right)) => $fn(left, right),
            (Array::UInt32(left), Array::UInt32(right)) => $fn(left, right),
            (Array::UInt64(left), Array::UInt64(right)) => $fn(left, right),
            _ => Err(RayexecError::new(
                "Unsupported arithmetic operation on array",
            )),
        }
    }};
}

impl CmpKernel for Array {
    type Output = BooleanArray;

    fn eq(&self, right: &Self) -> Result<Self::Output> {
        array_cmp_dispatch!(self, right, CmpKernel::eq)
    }

    fn neq(&self, right: &Self) -> Result<Self::Output> {
        array_cmp_dispatch!(self, right, CmpKernel::neq)
    }

    fn lt(&self, right: &Self) -> Result<Self::Output> {
        array_cmp_dispatch!(self, right, CmpKernel::lt)
    }

    fn lt_eq(&self, right: &Self) -> Result<Self::Output> {
        array_cmp_dispatch!(self, right, CmpKernel::lt_eq)
    }

    fn gt(&self, right: &Self) -> Result<Self::Output> {
        array_cmp_dispatch!(self, right, CmpKernel::gt)
    }

    fn gt_eq(&self, right: &Self) -> Result<Self::Output> {
        array_cmp_dispatch!(self, right, CmpKernel::gt_eq)
    }
}

impl<T: PartialEq + PartialOrd> CmpKernel for PrimitiveArray<T> {
    type Output = BooleanArray;

    fn eq(&self, right: &Self) -> Result<Self::Output> {
        primitive_array_cmp(self, right, PartialEq::eq)
    }

    fn neq(&self, right: &Self) -> Result<Self::Output> {
        primitive_array_cmp(self, right, PartialEq::ne)
    }

    fn lt(&self, right: &Self) -> Result<Self::Output> {
        primitive_array_cmp(self, right, PartialOrd::lt)
    }

    fn lt_eq(&self, right: &Self) -> Result<Self::Output> {
        primitive_array_cmp(self, right, PartialOrd::le)
    }

    fn gt(&self, right: &Self) -> Result<Self::Output> {
        primitive_array_cmp(self, right, PartialOrd::gt)
    }

    fn gt_eq(&self, right: &Self) -> Result<Self::Output> {
        primitive_array_cmp(self, right, PartialOrd::ge)
    }
}

// TODO: Varlens
macro_rules! scalar_cmp_dispatch {
    ($left:ident, $right:ident, $fn:expr) => {{
        match ($left, $right) {
            (ScalarValue::Float32(left), ScalarValue::Float32(right)) => $fn(left, right),
            (ScalarValue::Float64(left), ScalarValue::Float64(right)) => $fn(left, right),
            (ScalarValue::Int8(left), ScalarValue::Int8(right)) => $fn(left, right),
            (ScalarValue::Int16(left), ScalarValue::Int16(right)) => $fn(left, right),
            (ScalarValue::Int32(left), ScalarValue::Int32(right)) => $fn(left, right),
            (ScalarValue::Int64(left), ScalarValue::Int64(right)) => $fn(left, right),
            (ScalarValue::UInt8(left), ScalarValue::UInt8(right)) => $fn(left, right),
            (ScalarValue::UInt16(left), ScalarValue::UInt16(right)) => $fn(left, right),
            (ScalarValue::UInt32(left), ScalarValue::UInt32(right)) => $fn(left, right),
            (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => $fn(left, right),
            _ => Err(RayexecError::new(
                "Unsupported arithmetic operation on scalar",
            )),
        }
    }};
}

impl<'a> CmpKernel for ScalarValue<'a> {
    type Output = bool;

    fn eq(&self, right: &Self) -> Result<Self::Output> {
        scalar_cmp_dispatch!(self, right, |l, r| { Ok(PartialEq::eq(l, r)) })
    }

    fn neq(&self, right: &Self) -> Result<Self::Output> {
        scalar_cmp_dispatch!(self, right, |l, r| { Ok(PartialEq::ne(l, r)) })
    }

    fn lt(&self, right: &Self) -> Result<Self::Output> {
        scalar_cmp_dispatch!(self, right, |l, r| { Ok(PartialOrd::lt(l, r)) })
    }

    fn lt_eq(&self, right: &Self) -> Result<Self::Output> {
        scalar_cmp_dispatch!(self, right, |l, r| { Ok(PartialOrd::le(l, r)) })
    }

    fn gt(&self, right: &Self) -> Result<Self::Output> {
        scalar_cmp_dispatch!(self, right, |l, r| { Ok(PartialOrd::gt(l, r)) })
    }

    fn gt_eq(&self, right: &Self) -> Result<Self::Output> {
        scalar_cmp_dispatch!(self, right, |l, r| { Ok(PartialOrd::ge(l, r)) })
    }
}

fn primitive_array_cmp<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    cmp_fn: F,
) -> Result<BooleanArray>
where
    F: Fn(&T, &T) -> bool,
{
    if left.len() != right.len() {
        return Err(RayexecError::new(
            "Left and right arrays have different lengths",
        ));
    }

    let left = match left.values() {
        PrimitiveStorage::Vec(v) => v.iter(),
    };
    let right = match right.values() {
        PrimitiveStorage::Vec(v) => v.iter(),
    };

    let bools = compare_value_iters(left, right, cmp_fn);

    // TODO: Nulls

    Ok(bools)
}

/// Compare the the values from two iterators with some comparison function and
/// return a boolean array containing the results.
fn compare_value_iters<T, F>(
    left: impl Iterator<Item = T>,
    right: impl Iterator<Item = T>,
    cmp_fn: F,
) -> BooleanArray
where
    F: Fn(T, T) -> bool,
{
    let iter = left.zip(right).map(|(left, right)| cmp_fn(left, right));
    let bitmap = Bitmap::from_bool_iter(iter);
    BooleanArray::new_with_values(bitmap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_scalar_lt() {
        let left = ScalarValue::Int32(4);
        let right = ScalarValue::Int32(5);

        let out = left.lt(&right).unwrap();
        assert_eq!(true, out);
    }
}
