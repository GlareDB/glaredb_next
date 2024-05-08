use super::{
    specialize_check_num_args, specialize_invalid_input_type, GenericScalarFunction, InputTypes,
    ScalarFn, Signature, SpecializedScalarFunction,
};
use rayexec_bullet::array::{BooleanArrayBuilder, PrimitiveArrayBuilder, VarlenArrayBuilder};
use rayexec_bullet::executor::{BinaryExecutor, UnaryExecutor};
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;

#[derive(Debug, Clone, Copy)]
pub struct Repeat;

impl GenericScalarFunction for Repeat {
    fn name(&self) -> &str {
        "repeat"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Utf8, DataType::Int64]),
                return_type: DataType::Utf8,
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Utf8, DataType::Int64]),
                return_type: DataType::Utf8,
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Int64) => Ok(Box::new(RepeatUtf8)),
            (DataType::LargeUtf8, DataType::Int64) => Ok(Box::new(RepeatLargeUtf8)),
            (a, b) => Err(specialize_invalid_input_type(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RepeatUtf8;

impl SpecializedScalarFunction for RepeatUtf8 {
    fn function_impl(&self) -> ScalarFn {
        fn repeat_utf8(arrays: &[&Array]) -> Result<Array> {
            let strings = arrays[0];
            let nums = arrays[1];
            Ok(match (strings, nums) {
                (Array::Utf8(strings), Array::Int64(nums)) => {
                    let mut builder = VarlenArrayBuilder::new();
                    BinaryExecutor::execute(
                        strings,
                        nums,
                        |s, count| s.repeat(count as usize),
                        &mut builder,
                    )?;
                    Array::Utf8(builder.into_typed_array())
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        repeat_utf8
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RepeatLargeUtf8;

impl SpecializedScalarFunction for RepeatLargeUtf8 {
    fn function_impl(&self) -> ScalarFn {
        fn repeat_large_utf8(arrays: &[&Array]) -> Result<Array> {
            let strings = arrays[0];
            let nums = arrays[1];
            Ok(match (strings, nums) {
                (Array::LargeUtf8(strings), Array::Int64(nums)) => {
                    let mut builder = VarlenArrayBuilder::new();
                    BinaryExecutor::execute(
                        strings,
                        nums,
                        |s, count| s.repeat(count as usize),
                        &mut builder,
                    )?;
                    Array::LargeUtf8(builder.into_typed_array())
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        repeat_large_utf8
    }
}
