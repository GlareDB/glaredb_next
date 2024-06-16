use crate::functions::scalar::macros::primitive_unary_execute;
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, Signature,
};
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;
use std::sync::Arc;

use super::{GenericScalarFunction, ScalarFn, SpecializedScalarFunction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Negate;

impl FunctionInfo for Negate {
    fn name(&self) -> &'static str {
        "negate"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataType::Float32],
                return_type: DataType::Float32,
            },
            Signature {
                input: &[DataType::Float64],
                return_type: DataType::Float64,
            },
            Signature {
                input: &[DataType::Int8],
                return_type: DataType::Int8,
            },
            Signature {
                input: &[DataType::Int16],
                return_type: DataType::Int16,
            },
            Signature {
                input: &[DataType::Int32],
                return_type: DataType::Int32,
            },
            Signature {
                input: &[DataType::Int64],
                return_type: DataType::Int64,
            },
            Signature {
                input: &[DataType::Interval],
                return_type: DataType::Interval,
            },
        ]
    }
}

impl GenericScalarFunction for Negate {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => Ok(Box::new(NegatePrimitiveSpecialized)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NegatePrimitiveSpecialized;

impl SpecializedScalarFunction for NegatePrimitiveSpecialized {
    fn function_impl(&self) -> ScalarFn {
        fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            Ok(match first.as_ref() {
                Array::Int8(input) => {
                    primitive_unary_execute!(input, Int8, |a| -a)
                }
                Array::Int16(input) => {
                    primitive_unary_execute!(input, Int16, |a| -a)
                }
                Array::Int32(input) => {
                    primitive_unary_execute!(input, Int32, |a| -a)
                }
                Array::Int64(input) => {
                    primitive_unary_execute!(input, Int64, |a| -a)
                }
                Array::Float32(input) => {
                    primitive_unary_execute!(input, Float32, |a| -a)
                }
                Array::Float64(input) => {
                    primitive_unary_execute!(input, Float64, |a| -a)
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        inner
    }
}
