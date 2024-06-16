use crate::functions::scalar::macros::{
    primitive_binary_execute, primitive_binary_execute_no_wrap,
};
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, Signature,
};

use super::{GenericScalarFunction, ScalarFn, SpecializedScalarFunction};
use rayexec_bullet::array::{Array, Decimal128Array, Decimal64Array};
use rayexec_bullet::array::{Interval, PrimitiveArray};
use rayexec_bullet::datatype::{DataType, TypeMeta};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

/// Signatures for primitive arith operations (+, -, /, *, %)
// TODO: This needs to be placed directly into the functions and not shared
// since some operations apply to intervals/dates, but not others.
const PRIMITIVE_ARITH_SIGNATURES: &[Signature] = &[
    Signature {
        input: &[DataType::Float32, DataType::Float32],
        return_type: DataType::Float32,
    },
    Signature {
        input: &[DataType::Float64, DataType::Float64],
        return_type: DataType::Float64,
    },
    Signature {
        input: &[DataType::Int8, DataType::Int8],
        return_type: DataType::Int8,
    },
    Signature {
        input: &[DataType::Int16, DataType::Int16],
        return_type: DataType::Int16,
    },
    Signature {
        input: &[DataType::Int32, DataType::Int32],
        return_type: DataType::Int32,
    },
    Signature {
        input: &[DataType::Int64, DataType::Int64],
        return_type: DataType::Int64,
    },
    Signature {
        input: &[DataType::UInt8, DataType::UInt8],
        return_type: DataType::UInt8,
    },
    Signature {
        input: &[DataType::UInt16, DataType::UInt16],
        return_type: DataType::UInt16,
    },
    Signature {
        input: &[DataType::UInt32, DataType::UInt32],
        return_type: DataType::UInt32,
    },
    Signature {
        input: &[DataType::UInt64, DataType::UInt64],
        return_type: DataType::UInt64,
    },
    Signature {
        input: &[DataType::Date32, DataType::Int64],
        return_type: DataType::Date32,
    },
    Signature {
        input: &[DataType::Interval, DataType::Int64],
        return_type: DataType::Interval,
    },
    Signature {
        input: &[
            DataType::Decimal64(TypeMeta::None),
            DataType::Decimal64(TypeMeta::None),
        ],
        return_type: DataType::Decimal64(TypeMeta::None),
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Add;

impl FunctionInfo for Add {
    fn name(&self) -> &'static str {
        "+"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["add"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }
}

impl GenericScalarFunction for Add {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Date32, DataType::Int64) => Ok(Box::new(AddPrimitiveSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddPrimitiveSpecialized;

impl SpecializedScalarFunction for AddPrimitiveSpecialized {
    fn function_impl(&self) -> ScalarFn {
        fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Int8(first), Array::Int8(second)) => {
                    primitive_binary_execute!(first, second, Int8, |a, b| a + b)
                }
                (Array::Int16(first), Array::Int16(second)) => {
                    primitive_binary_execute!(first, second, Int16, |a, b| a + b)
                }
                (Array::Int32(first), Array::Int32(second)) => {
                    primitive_binary_execute!(first, second, Int32, |a, b| a + b)
                }
                (Array::Int64(first), Array::Int64(second)) => {
                    primitive_binary_execute!(first, second, Int64, |a, b| a + b)
                }
                (Array::UInt8(first), Array::UInt8(second)) => {
                    primitive_binary_execute!(first, second, UInt8, |a, b| a + b)
                }
                (Array::UInt16(first), Array::UInt16(second)) => {
                    primitive_binary_execute!(first, second, UInt16, |a, b| a + b)
                }
                (Array::UInt32(first), Array::UInt32(second)) => {
                    primitive_binary_execute!(first, second, UInt32, |a, b| a + b)
                }
                (Array::UInt64(first), Array::UInt64(second)) => {
                    primitive_binary_execute!(first, second, UInt64, |a, b| a + b)
                }
                (Array::Float32(first), Array::Float32(second)) => {
                    primitive_binary_execute!(first, second, Float32, |a, b| a + b)
                }
                (Array::Float64(first), Array::Float64(second)) => {
                    primitive_binary_execute!(first, second, Float64, |a, b| a + b)
                }
                (Array::Decimal64(first), Array::Decimal64(second)) => {
                    // TODO: Scale
                    Decimal64Array::new(
                        first.precision(),
                        first.scale(),
                        primitive_binary_execute_no_wrap!(
                            first.get_primitive(),
                            second.get_primitive(),
                            |a, b| a + b
                        ),
                    )
                    .into()
                }
                (Array::Decimal128(first), Array::Decimal128(second)) => {
                    // TODO: Scale
                    Decimal128Array::new(
                        first.precision(),
                        first.scale(),
                        primitive_binary_execute_no_wrap!(
                            first.get_primitive(),
                            second.get_primitive(),
                            |a, b| a + b
                        ),
                    )
                    .into()
                }
                (Array::Date32(first), Array::Int64(second)) => {
                    // Date32 is stored as "days", so just add the values.
                    primitive_binary_execute!(first, second, Date32, |a, b| a + b as i32)
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sub;

impl FunctionInfo for Sub {
    fn name(&self) -> &'static str {
        "-"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["sub"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }
}

impl GenericScalarFunction for Sub {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Date32, DataType::Int64) => Ok(Box::new(SubPrimitiveSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubPrimitiveSpecialized;

impl SpecializedScalarFunction for SubPrimitiveSpecialized {
    fn function_impl(&self) -> ScalarFn {
        fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Int8(first), Array::Int8(second)) => {
                    primitive_binary_execute!(first, second, Int8, |a, b| a - b)
                }
                (Array::Int16(first), Array::Int16(second)) => {
                    primitive_binary_execute!(first, second, Int16, |a, b| a - b)
                }
                (Array::Int32(first), Array::Int32(second)) => {
                    primitive_binary_execute!(first, second, Int32, |a, b| a - b)
                }
                (Array::Int64(first), Array::Int64(second)) => {
                    primitive_binary_execute!(first, second, Int64, |a, b| a - b)
                }
                (Array::UInt8(first), Array::UInt8(second)) => {
                    primitive_binary_execute!(first, second, UInt8, |a, b| a - b)
                }
                (Array::UInt16(first), Array::UInt16(second)) => {
                    primitive_binary_execute!(first, second, UInt16, |a, b| a - b)
                }
                (Array::UInt32(first), Array::UInt32(second)) => {
                    primitive_binary_execute!(first, second, UInt32, |a, b| a - b)
                }
                (Array::UInt64(first), Array::UInt64(second)) => {
                    primitive_binary_execute!(first, second, UInt64, |a, b| a - b)
                }
                (Array::Float32(first), Array::Float32(second)) => {
                    primitive_binary_execute!(first, second, Float32, |a, b| a - b)
                }
                (Array::Float64(first), Array::Float64(second)) => {
                    primitive_binary_execute!(first, second, Float64, |a, b| a - b)
                }
                (Array::Decimal64(first), Array::Decimal64(second)) => {
                    // TODO: Scale
                    Decimal64Array::new(
                        first.precision(),
                        first.scale(),
                        primitive_binary_execute_no_wrap!(
                            first.get_primitive(),
                            second.get_primitive(),
                            |a, b| a - b
                        ),
                    )
                    .into()
                }
                (Array::Decimal128(first), Array::Decimal128(second)) => {
                    // TODO: Scale
                    Decimal128Array::new(
                        first.precision(),
                        first.scale(),
                        primitive_binary_execute_no_wrap!(
                            first.get_primitive(),
                            second.get_primitive(),
                            |a, b| a - b
                        ),
                    )
                    .into()
                }
                (Array::Date32(first), Array::Int64(second)) => {
                    // Date32 is stored as "days", so just sub the values.
                    primitive_binary_execute!(first, second, Date32, |a, b| a - b as i32)
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Div;

impl FunctionInfo for Div {
    fn name(&self) -> &'static str {
        "/"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["div"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }
}

impl GenericScalarFunction for Div {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Date32, DataType::Int64) => Ok(Box::new(DivPrimitiveSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DivPrimitiveSpecialized;

impl SpecializedScalarFunction for DivPrimitiveSpecialized {
    fn function_impl(&self) -> ScalarFn {
        fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Int8(first), Array::Int8(second)) => {
                    primitive_binary_execute!(first, second, Int8, |a, b| a / b)
                }
                (Array::Int16(first), Array::Int16(second)) => {
                    primitive_binary_execute!(first, second, Int16, |a, b| a / b)
                }
                (Array::Int32(first), Array::Int32(second)) => {
                    primitive_binary_execute!(first, second, Int32, |a, b| a / b)
                }
                (Array::Int64(first), Array::Int64(second)) => {
                    primitive_binary_execute!(first, second, Int64, |a, b| a / b)
                }
                (Array::UInt8(first), Array::UInt8(second)) => {
                    primitive_binary_execute!(first, second, UInt8, |a, b| a / b)
                }
                (Array::UInt16(first), Array::UInt16(second)) => {
                    primitive_binary_execute!(first, second, UInt16, |a, b| a / b)
                }
                (Array::UInt32(first), Array::UInt32(second)) => {
                    primitive_binary_execute!(first, second, UInt32, |a, b| a / b)
                }
                (Array::UInt64(first), Array::UInt64(second)) => {
                    primitive_binary_execute!(first, second, UInt64, |a, b| a / b)
                }
                (Array::Float32(first), Array::Float32(second)) => {
                    primitive_binary_execute!(first, second, Float32, |a, b| a / b)
                }
                (Array::Float64(first), Array::Float64(second)) => {
                    primitive_binary_execute!(first, second, Float64, |a, b| a / b)
                }
                (Array::Decimal64(first), Array::Decimal64(second)) => {
                    // TODO: Scale
                    Decimal64Array::new(
                        first.precision(),
                        first.scale(),
                        primitive_binary_execute_no_wrap!(
                            first.get_primitive(),
                            second.get_primitive(),
                            |a, b| a / b
                        ),
                    )
                    .into()
                }
                (Array::Decimal128(first), Array::Decimal128(second)) => {
                    // TODO: Scale
                    Decimal128Array::new(
                        first.precision(),
                        first.scale(),
                        primitive_binary_execute_no_wrap!(
                            first.get_primitive(),
                            second.get_primitive(),
                            |a, b| a / b
                        ),
                    )
                    .into()
                }

                other => panic!("unexpected array type: {other:?}"),
            })
        }

        inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mul;

impl FunctionInfo for Mul {
    fn name(&self) -> &'static str {
        "*"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["mul"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }
}

impl GenericScalarFunction for Mul {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Date32, DataType::Int64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Interval, DataType::Int64) => Ok(Box::new(MulPrimitiveSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MulPrimitiveSpecialized;

impl SpecializedScalarFunction for MulPrimitiveSpecialized {
    fn function_impl(&self) -> ScalarFn {
        fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Int8(first), Array::Int8(second)) => {
                    primitive_binary_execute!(first, second, Int8, |a, b| a * b)
                }
                (Array::Int16(first), Array::Int16(second)) => {
                    primitive_binary_execute!(first, second, Int16, |a, b| a * b)
                }
                (Array::Int32(first), Array::Int32(second)) => {
                    primitive_binary_execute!(first, second, Int32, |a, b| a * b)
                }
                (Array::Int64(first), Array::Int64(second)) => {
                    primitive_binary_execute!(first, second, Int64, |a, b| a * b)
                }
                (Array::UInt8(first), Array::UInt8(second)) => {
                    primitive_binary_execute!(first, second, UInt8, |a, b| a * b)
                }
                (Array::UInt16(first), Array::UInt16(second)) => {
                    primitive_binary_execute!(first, second, UInt16, |a, b| a * b)
                }
                (Array::UInt32(first), Array::UInt32(second)) => {
                    primitive_binary_execute!(first, second, UInt32, |a, b| a * b)
                }
                (Array::UInt64(first), Array::UInt64(second)) => {
                    primitive_binary_execute!(first, second, UInt64, |a, b| a * b)
                }
                (Array::Float32(first), Array::Float32(second)) => {
                    primitive_binary_execute!(first, second, Float32, |a, b| a * b)
                }
                (Array::Float64(first), Array::Float64(second)) => {
                    primitive_binary_execute!(first, second, Float64, |a, b| a * b)
                }
                (Array::Decimal64(first), Array::Decimal64(second)) => {
                    // TODO: Scale
                    Decimal64Array::new(
                        first.precision(),
                        first.scale(),
                        primitive_binary_execute_no_wrap!(
                            first.get_primitive(),
                            second.get_primitive(),
                            |a, b| a * b
                        ),
                    )
                    .into()
                }
                (Array::Decimal128(first), Array::Decimal128(second)) => {
                    // TODO: Scale
                    Decimal128Array::new(
                        first.precision(),
                        first.scale(),
                        primitive_binary_execute_no_wrap!(
                            first.get_primitive(),
                            second.get_primitive(),
                            |a, b| a * b
                        ),
                    )
                    .into()
                }
                (Array::Interval(first), Array::Int64(second)) => {
                    primitive_binary_execute!(first, second, Interval, |a, b| {
                        Interval {
                            months: a.months * (b as i32),
                            days: a.days * (b as i32),
                            nanos: a.nanos * b,
                        }
                    })
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rem;

impl FunctionInfo for Rem {
    fn name(&self) -> &'static str {
        "%"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["rem", "mod"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }
}

impl GenericScalarFunction for Rem {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Date32, DataType::Int64)
            | (DataType::Interval, DataType::Int64) => Ok(Box::new(RemPrimitiveSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemPrimitiveSpecialized;

impl SpecializedScalarFunction for RemPrimitiveSpecialized {
    fn function_impl(&self) -> ScalarFn {
        fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Int8(first), Array::Int8(second)) => {
                    primitive_binary_execute!(first, second, Int8, |a, b| a % b)
                }
                (Array::Int16(first), Array::Int16(second)) => {
                    primitive_binary_execute!(first, second, Int16, |a, b| a % b)
                }
                (Array::Int32(first), Array::Int32(second)) => {
                    primitive_binary_execute!(first, second, Int32, |a, b| a % b)
                }
                (Array::Int64(first), Array::Int64(second)) => {
                    primitive_binary_execute!(first, second, Int64, |a, b| a % b)
                }
                (Array::UInt8(first), Array::UInt8(second)) => {
                    primitive_binary_execute!(first, second, UInt8, |a, b| a % b)
                }
                (Array::UInt16(first), Array::UInt16(second)) => {
                    primitive_binary_execute!(first, second, UInt16, |a, b| a % b)
                }
                (Array::UInt32(first), Array::UInt32(second)) => {
                    primitive_binary_execute!(first, second, UInt32, |a, b| a % b)
                }
                (Array::UInt64(first), Array::UInt64(second)) => {
                    primitive_binary_execute!(first, second, UInt64, |a, b| a % b)
                }
                (Array::Float32(first), Array::Float32(second)) => {
                    primitive_binary_execute!(first, second, Float32, |a, b| a % b)
                }
                (Array::Float64(first), Array::Float64(second)) => {
                    primitive_binary_execute!(first, second, Float64, |a, b| a % b)
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        inner
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::Int32Array;

    use super::*;

    #[test]
    fn add_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));

        let specialized = Add.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([5, 7, 9]));

        assert_eq!(expected, out);
    }

    #[test]
    fn sub_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Sub.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([3, 3, 3]));

        assert_eq!(expected, out);
    }

    #[test]
    fn div_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Div.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([4, 2, 2]));

        assert_eq!(expected, out);
    }

    #[test]
    fn rem_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Rem.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([0, 1, 0]));

        assert_eq!(expected, out);
    }

    #[test]
    fn mul_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Mul.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([4, 10, 18]));

        assert_eq!(expected, out);
    }
}
