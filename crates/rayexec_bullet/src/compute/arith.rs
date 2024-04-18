use crate::array::{Array, PrimitiveArray};
use crate::scalar::ScalarValue;
use crate::storage::PrimitiveStorage;
use rayexec_error::{RayexecError, Result};
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Rem, RemAssign, Sub, SubAssign};

/// Arithmetic operations that assign the result into the left-hand side.
pub trait ArithAssign<Rhs = Self> {
    fn add(&mut self, right: &Rhs) -> Result<()>;
    fn checked_add(&mut self, right: &Rhs) -> Result<()>;

    fn sub(&mut self, right: &Rhs) -> Result<()>;
    fn checked_sub(&mut self, right: &Rhs) -> Result<()>;

    fn mul(&mut self, right: &Rhs) -> Result<()>;
    fn checked_mul(&mut self, right: &Rhs) -> Result<()>;

    fn div(&mut self, right: &Rhs) -> Result<()>;
    fn checked_div(&mut self, right: &Rhs) -> Result<()>;

    fn rem(&mut self, right: &Rhs) -> Result<()>;
}

macro_rules! array_arith_dispatch {
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

impl ArithAssign for Array {
    fn add(&mut self, right: &Self) -> Result<()> {
        array_arith_dispatch!(self, right, ArithAssign::add)
    }

    fn checked_add(&mut self, right: &Self) -> Result<()> {
        // TODO
        array_arith_dispatch!(self, right, ArithAssign::add)
    }

    fn sub(&mut self, right: &Self) -> Result<()> {
        array_arith_dispatch!(self, right, ArithAssign::sub)
    }

    fn checked_sub(&mut self, right: &Self) -> Result<()> {
        // TODO
        array_arith_dispatch!(self, right, ArithAssign::sub)
    }

    fn mul(&mut self, right: &Self) -> Result<()> {
        array_arith_dispatch!(self, right, ArithAssign::mul)
    }

    fn checked_mul(&mut self, right: &Self) -> Result<()> {
        // TODO
        array_arith_dispatch!(self, right, ArithAssign::mul)
    }

    fn div(&mut self, right: &Self) -> Result<()> {
        array_arith_dispatch!(self, right, ArithAssign::div)
    }

    fn checked_div(&mut self, right: &Self) -> Result<()> {
        // TODO
        array_arith_dispatch!(self, right, ArithAssign::div)
    }

    fn rem(&mut self, right: &Self) -> Result<()> {
        array_arith_dispatch!(self, right, ArithAssign::rem)
    }
}

macro_rules! scalar_arith_dispatch {
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
                "Unsupported arithmetic operation on array",
            )),
        }
    }};
}

impl<'a> ArithAssign for ScalarValue<'a> {
    fn add(&mut self, right: &Self) -> Result<()> {
        scalar_arith_dispatch!(self, right, |l, r| {
            AddAssign::add_assign(l, r);
            Ok(())
        })
    }

    fn checked_add(&mut self, right: &Self) -> Result<()> {
        unimplemented!()
    }

    fn sub(&mut self, right: &Self) -> Result<()> {
        scalar_arith_dispatch!(self, right, |l, r| {
            SubAssign::sub_assign(l, r);
            Ok(())
        })
    }

    fn checked_sub(&mut self, right: &Self) -> Result<()> {
        unimplemented!()
    }

    fn mul(&mut self, right: &Self) -> Result<()> {
        scalar_arith_dispatch!(self, right, |l, r| {
            MulAssign::mul_assign(l, r);
            Ok(())
        })
    }

    fn checked_mul(&mut self, right: &Self) -> Result<()> {
        unimplemented!()
    }

    fn div(&mut self, right: &Self) -> Result<()> {
        scalar_arith_dispatch!(self, right, |l, r| {
            DivAssign::div_assign(l, r);
            Ok(())
        })
    }

    fn checked_div(&mut self, right: &Self) -> Result<()> {
        unimplemented!()
    }

    fn rem(&mut self, right: &Self) -> Result<()> {
        scalar_arith_dispatch!(self, right, |l, r| {
            RemAssign::rem_assign(l, r);
            Ok(())
        })
    }
}

impl<
        T: Add<Output = T>
            + Sub<Output = T>
            + Mul<Output = T>
            + Div<Output = T>
            + Rem<Output = T>
            + Copy,
    > ArithAssign for PrimitiveArray<T>
{
    fn add(&mut self, right: &Self) -> Result<()> {
        primitive_bin_op_assign(self, right, Add::add)
    }

    fn checked_add(&mut self, right: &Self) -> Result<()> {
        unimplemented!()
    }

    fn sub(&mut self, right: &Self) -> Result<()> {
        primitive_bin_op_assign(self, right, Sub::sub)
    }

    fn checked_sub(&mut self, right: &Self) -> Result<()> {
        unimplemented!()
    }

    fn mul(&mut self, right: &Self) -> Result<()> {
        primitive_bin_op_assign(self, right, Mul::mul)
    }

    fn checked_mul(&mut self, right: &Self) -> Result<()> {
        unimplemented!()
    }

    fn div(&mut self, right: &Self) -> Result<()> {
        primitive_bin_op_assign(self, right, Div::div)
    }

    fn checked_div(&mut self, right: &Self) -> Result<()> {
        unimplemented!()
    }

    fn rem(&mut self, right: &Self) -> Result<()> {
        primitive_bin_op_assign(self, right, Rem::rem)
    }
}

/// Execute a binary function on left and right, assigning the result to left.
fn primitive_bin_op_assign<T, F>(
    left: &mut PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    f: F,
) -> Result<()>
where
    T: Copy,
    F: Fn(T, T) -> T,
{
    if left.len() != right.len() {
        return Err(RayexecError::new(
            "Left and right arrays have different lengths",
        ));
    }

    let left = match left.values_mut() {
        PrimitiveStorage::Vec(v) => v.iter_mut(),
    };
    let right = match right.values() {
        PrimitiveStorage::Vec(v) => v.iter(),
    };

    for (left, right) in left.zip(right) {
        *left = f(*left, *right);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_scalar_add() {
        // Sanity check

        let mut left = ScalarValue::Int32(5);
        let right = ScalarValue::Int32(8);

        left.add(&right).unwrap();

        assert_eq!(ScalarValue::Int32(13), left);
    }
}
