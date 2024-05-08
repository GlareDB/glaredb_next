use super::{
    specialize_check_num_args, specialize_invalid_input_type, GenericScalarFunction, InputTypes,
    ScalarFn, Signature, SpecializedScalarFunction,
};
use rayexec_bullet::array::{BooleanArrayBuilder, PrimitiveArrayBuilder};
use rayexec_bullet::executor::{BinaryExecutor, UnaryExecutor};
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;

/// Macro for generating a specialized unary function that accepts a primitive
/// array of some variant, and produces a primitive array of some variant.
///
/// Operation should be a lambda accepting one input, and producing one output
/// of the expected type.
macro_rules! generate_specialized_unary_numeric {
    ($name:ident, $input_variant:ident, $output_variant:ident, $operation:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name;

        impl SpecializedScalarFunction for $name {
            fn function_impl(&self) -> ScalarFn {
                fn inner(arrays: &[&Array]) -> Result<Array> {
                    let array = arrays[0];
                    Ok(match array {
                        Array::$input_variant(array) => {
                            let mut builder = PrimitiveArrayBuilder::with_capacity(array.len());
                            UnaryExecutor::execute(array, $operation, &mut builder)?;
                            Array::$output_variant(builder.into_typed_array())
                        }
                        other => panic!("unexpected array type: {other:?}"),
                    })
                }

                inner
            }
        }
    };
}

/// Macro for generating a specialized binary function that accepts two
/// primitive arrays, and produces a single primitive array.
///
/// The operation should accept two inputs, producing a single output of the
/// expected type.
macro_rules! generate_specialized_binary_numeric {
    ($name:ident, $first_variant:ident, $second_variant:ident, $output_variant:ident, $operation:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name;

        impl SpecializedScalarFunction for $name {
            fn function_impl(&self) -> ScalarFn {
                fn inner(arrays: &[&Array]) -> Result<Array> {
                    let first = arrays[0];
                    let second = arrays[1];
                    Ok(match (first, second) {
                        (Array::$first_variant(first), Array::$second_variant(second)) => {
                            let mut builder = PrimitiveArrayBuilder::with_capacity(first.len());
                            BinaryExecutor::execute(first, second, $operation, &mut builder)?;
                            Array::$output_variant(builder.into_typed_array())
                        }
                        other => panic!("unexpected array type: {other:?}"),
                    })
                }

                inner
            }
        }
    };
}

#[derive(Debug, Clone, Copy)]
pub struct IsNan;

impl GenericScalarFunction for IsNan {
    fn name(&self) -> &str {
        "isnan"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: DataType::Boolean,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: DataType::Boolean,
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(IsNanFloat32)),
            DataType::Float64 => Ok(Box::new(IsNanFloat64)),
            other => Err(specialize_invalid_input_type(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IsNanFloat32;

impl SpecializedScalarFunction for IsNanFloat32 {
    fn function_impl(&self) -> ScalarFn {
        fn is_nan_f32_impl(arrays: &[&Array]) -> Result<Array> {
            let array = arrays[0];
            Ok(match array {
                Array::Float32(array) => {
                    let mut builder = BooleanArrayBuilder::new();
                    UnaryExecutor::execute(array, |f| f.is_nan(), &mut builder)?;
                    Array::Boolean(builder.into_typed_array())
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        is_nan_f32_impl
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IsNanFloat64;

impl SpecializedScalarFunction for IsNanFloat64 {
    fn function_impl(&self) -> ScalarFn {
        fn is_nan_f64_impl(arrays: &[&Array]) -> Result<Array> {
            let array = arrays[0];
            Ok(match array {
                Array::Float64(array) => {
                    let mut builder = BooleanArrayBuilder::new();
                    UnaryExecutor::execute(array, |f| f.is_nan(), &mut builder)?;
                    Array::Boolean(builder.into_typed_array())
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        is_nan_f64_impl
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Ceil;

impl GenericScalarFunction for Ceil {
    fn name(&self) -> &str {
        "ceil"
    }

    fn aliases(&self) -> &[&str] {
        &["ceiling"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: DataType::Float32,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: DataType::Float64,
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(CeilFloat32)),
            DataType::Float64 => Ok(Box::new(CeilFloat64)),
            other => Err(specialize_invalid_input_type(self, &[other])),
        }
    }
}

generate_specialized_unary_numeric!(CeilFloat32, Float32, Float32, |f| f.ceil());
generate_specialized_unary_numeric!(CeilFloat64, Float64, Float64, |f| f.ceil());

#[derive(Debug, Clone, Copy)]
pub struct Floor;

impl GenericScalarFunction for Floor {
    fn name(&self) -> &str {
        "floor"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: DataType::Float32,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: DataType::Float64,
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(FloorFloat32)),
            DataType::Float64 => Ok(Box::new(FloorFloat64)),
            other => Err(specialize_invalid_input_type(self, &[other])),
        }
    }
}

generate_specialized_unary_numeric!(FloorFloat32, Float32, Float32, |f| f.ceil());
generate_specialized_unary_numeric!(FloorFloat64, Float64, Float64, |f| f.ceil());

#[derive(Debug, Clone, Copy)]
pub struct Add;

impl GenericScalarFunction for Add {
    fn name(&self) -> &str {
        "+"
    }

    fn aliases(&self) -> &[&str] {
        &["add"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32, DataType::Float32]),
                return_type: DataType::Float32,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64, DataType::Float64]),
                return_type: DataType::Float64,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int8, DataType::Int8]),
                return_type: DataType::Int8,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int16, DataType::Int16]),
                return_type: DataType::Int16,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int32, DataType::Int32]),
                return_type: DataType::Int32,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int64, DataType::Int64]),
                return_type: DataType::Int64,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::UInt8, DataType::UInt8]),
                return_type: DataType::UInt8,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::UInt16, DataType::UInt16]),
                return_type: DataType::UInt16,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::UInt32, DataType::UInt32]),
                return_type: DataType::UInt32,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::UInt64, DataType::UInt64]),
                return_type: DataType::UInt64,
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(AddFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(AddFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(AddInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(AddInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(AddInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(AddInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(AddUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(AddUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(AddUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(AddUInt64)),
            (a, b) => Err(specialize_invalid_input_type(self, &[a, b])),
        }
    }
}

generate_specialized_binary_numeric!(AddFloat32, Float32, Float32, Float32, |a, b| a + b);
generate_specialized_binary_numeric!(AddFloat64, Float64, Float64, Float64, |a, b| a + b);
generate_specialized_binary_numeric!(AddInt8, Int8, Int8, Int8, |a, b| a + b);
generate_specialized_binary_numeric!(AddInt16, Int16, Int16, Int16, |a, b| a + b);
generate_specialized_binary_numeric!(AddInt32, Int32, Int32, Int32, |a, b| a + b);
generate_specialized_binary_numeric!(AddInt64, Int64, Int64, Int64, |a, b| a + b);
generate_specialized_binary_numeric!(AddUInt8, UInt8, UInt8, UInt8, |a, b| a + b);
generate_specialized_binary_numeric!(AddUInt16, UInt16, UInt16, UInt16, |a, b| a + b);
generate_specialized_binary_numeric!(AddUInt32, UInt32, UInt32, UInt32, |a, b| a + b);
generate_specialized_binary_numeric!(AddUInt64, UInt64, UInt64, UInt64, |a, b| a + b);

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::Int32Array;

    use super::*;

    #[test]
    fn add_i32() {
        let a = Array::Int32(Int32Array::from_iter([1, 2, 3]));
        let b = Array::Int32(Int32Array::from_iter([4, 5, 6]));

        let specialized = Add.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([5, 7, 9]));

        assert_eq!(expected, out);
    }
}
