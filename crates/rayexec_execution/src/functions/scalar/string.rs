use super::{GenericScalarFunction, ScalarFn, SpecializedScalarFunction};
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, InputTypes, ReturnType,
    Signature,
};
use rayexec_bullet::array::{VarlenArray, VarlenValuesBuffer};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Repeat;

impl FunctionInfo for Repeat {
    fn name(&self) -> &'static str {
        "repeat"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Utf8, DataType::Int64]),
                return_type: ReturnType::Static(DataType::Utf8),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::LargeUtf8, DataType::Int64]),
                return_type: ReturnType::Static(DataType::LargeUtf8),
            },
        ]
    }
}

impl GenericScalarFunction for Repeat {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Int64) => Ok(Box::new(RepeatUtf8)),
            (DataType::LargeUtf8, DataType::Int64) => Ok(Box::new(RepeatLargeUtf8)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepeatUtf8;

impl SpecializedScalarFunction for RepeatUtf8 {
    fn function_impl(&self) -> ScalarFn {
        fn repeat_utf8(arrays: &[&Arc<Array>]) -> Result<Array> {
            let strings = arrays[0];
            let nums = arrays[1];
            Ok(match (strings.as_ref(), nums.as_ref()) {
                (Array::Utf8(strings), Array::Int64(nums)) => {
                    let mut buffer = VarlenValuesBuffer::default();
                    let validity = BinaryExecutor::execute(
                        strings,
                        nums,
                        |s, count| s.repeat(count as usize),
                        &mut buffer,
                    )?;
                    Array::Utf8(VarlenArray::new(buffer, validity))
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        repeat_utf8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepeatLargeUtf8;

impl SpecializedScalarFunction for RepeatLargeUtf8 {
    fn function_impl(&self) -> ScalarFn {
        fn repeat_large_utf8(arrays: &[&Arc<Array>]) -> Result<Array> {
            let strings = arrays[0];
            let nums = arrays[1];
            Ok(match (strings.as_ref(), nums.as_ref()) {
                (Array::LargeUtf8(strings), Array::Int64(nums)) => {
                    let mut buffer = VarlenValuesBuffer::default();
                    let validity = BinaryExecutor::execute(
                        strings,
                        nums,
                        |s, count| s.repeat(count as usize),
                        &mut buffer,
                    )?;
                    Array::LargeUtf8(VarlenArray::new(buffer, validity))
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        repeat_large_utf8
    }
}
