use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, InputTypes, ReturnType,
    Signature,
};
use rayexec_bullet::array::Array;
use rayexec_bullet::array::PrimitiveArray;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::scalar::UnaryExecutor;
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
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: ReturnType::Static(DataType::Float32),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: ReturnType::Static(DataType::Float64),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int8]),
                return_type: ReturnType::Static(DataType::Int8),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int16]),
                return_type: ReturnType::Static(DataType::Int16),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int32]),
                return_type: ReturnType::Static(DataType::Int32),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int64]),
                return_type: ReturnType::Static(DataType::Int64),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Interval]),
                return_type: ReturnType::Static(DataType::Interval),
            },
        ]
    }
}

impl GenericScalarFunction for Negate {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int8 => Ok(Box::new(NegateI8)),
            DataType::Int16 => Ok(Box::new(NegateI16)),
            DataType::Int32 => Ok(Box::new(NegateI32)),
            DataType::Int64 => Ok(Box::new(NegateI64)),
            DataType::Float32 => Ok(Box::new(NegateF32)),
            DataType::Float64 => Ok(Box::new(NegateF64)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

macro_rules! generate_specialized_unary_numeric {
    ($name:ident, $first_variant:ident, $output_variant:ident, $operation:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl SpecializedScalarFunction for $name {
            fn function_impl(&self) -> ScalarFn {
                fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
                    let first = arrays[0];
                    Ok(match first.as_ref() {
                        Array::$first_variant(first) => {
                            let mut buffer = Vec::with_capacity(first.len());
                            UnaryExecutor::execute(first, $operation, &mut buffer)?;
                            Array::$output_variant(PrimitiveArray::new(
                                buffer,
                                first.validity().cloned(),
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

generate_specialized_unary_numeric!(NegateI8, Int8, Int8, |v| -v);
generate_specialized_unary_numeric!(NegateI16, Int16, Int16, |v| -v);
generate_specialized_unary_numeric!(NegateI32, Int32, Int32, |v| -v);
generate_specialized_unary_numeric!(NegateI64, Int64, Int64, |v| -v);
generate_specialized_unary_numeric!(NegateF32, Float32, Float32, |v| -v);
generate_specialized_unary_numeric!(NegateF64, Float64, Float64, |v| -v);
