use super::{GenericScalarFunction, ScalarFn, SpecializedScalarFunction};
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, InputTypes, ReturnType,
    Signature,
};
use rayexec_bullet::array::{BooleanArray, BooleanValuesBuffer, PrimitiveArray};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

/// Macro for generating a specialized unary function that accepts a primitive
/// array of some variant, and produces a primitive array of some variant.
///
/// Operation should be a lambda accepting one input, and producing one output
/// of the expected type.
macro_rules! generate_specialized_unary_numeric {
    ($name:ident, $input_variant:ident, $output_variant:ident, $operation:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl SpecializedScalarFunction for $name {
            fn function_impl(&self) -> ScalarFn {
                fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
                    let array = arrays[0];
                    Ok(match array.as_ref() {
                        Array::$input_variant(array) => {
                            let mut buffer = Vec::with_capacity(array.len());
                            UnaryExecutor::execute(array, $operation, &mut buffer)?;
                            Array::$output_variant(PrimitiveArray::new(
                                buffer,
                                array.validity().cloned(),
                            ))
                        }
                        other => panic!("unexpected array type: {other:?}"),
                    })
                }

                inner
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNan;

impl FunctionInfo for IsNan {
    fn name(&self) -> &'static str {
        "isnan"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: ReturnType::Static(DataType::Boolean),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: ReturnType::Static(DataType::Boolean),
            },
        ]
    }
}

impl GenericScalarFunction for IsNan {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(IsNanFloat32)),
            DataType::Float64 => Ok(Box::new(IsNanFloat64)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNanFloat32;

impl SpecializedScalarFunction for IsNanFloat32 {
    fn function_impl(&self) -> ScalarFn {
        fn is_nan_f32_impl(arrays: &[&Arc<Array>]) -> Result<Array> {
            let array = arrays[0];
            Ok(match array.as_ref() {
                Array::Float32(array) => {
                    let mut buffer = BooleanValuesBuffer::with_capacity(array.len());
                    UnaryExecutor::execute(array, |f| f.is_nan(), &mut buffer)?;
                    Array::Boolean(BooleanArray::new(buffer, array.validity().cloned()))
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        is_nan_f32_impl
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNanFloat64;

impl SpecializedScalarFunction for IsNanFloat64 {
    fn function_impl(&self) -> ScalarFn {
        fn is_nan_f64_impl(arrays: &[&Arc<Array>]) -> Result<Array> {
            let array = arrays[0];
            Ok(match array.as_ref() {
                Array::Float64(array) => {
                    let mut buffer = BooleanValuesBuffer::with_capacity(array.len());
                    UnaryExecutor::execute(array, |f| f.is_nan(), &mut buffer)?;
                    Array::Boolean(BooleanArray::new(buffer, array.validity().cloned()))
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        is_nan_f64_impl
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ceil;

impl FunctionInfo for Ceil {
    fn name(&self) -> &'static str {
        "ceil"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["ceiling"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: ReturnType::Static(DataType::Float32),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: ReturnType::Static(DataType::Float64),
            },
        ]
    }
}

impl GenericScalarFunction for Ceil {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(CeilFloat32)),
            DataType::Float64 => Ok(Box::new(CeilFloat64)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

generate_specialized_unary_numeric!(CeilFloat32, Float32, Float32, |f| f.ceil());
generate_specialized_unary_numeric!(CeilFloat64, Float64, Float64, |f| f.ceil());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Floor;

impl FunctionInfo for Floor {
    fn name(&self) -> &'static str {
        "floor"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: ReturnType::Static(DataType::Float32),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: ReturnType::Static(DataType::Float64),
            },
        ]
    }
}

impl GenericScalarFunction for Floor {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(FloorFloat32)),
            DataType::Float64 => Ok(Box::new(FloorFloat64)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

generate_specialized_unary_numeric!(FloorFloat32, Float32, Float32, |f| f.ceil());
generate_specialized_unary_numeric!(FloorFloat64, Float64, Float64, |f| f.ceil());
