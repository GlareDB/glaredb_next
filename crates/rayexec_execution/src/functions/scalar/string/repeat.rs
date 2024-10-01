use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::Array2;
use rayexec_bullet::array::{VarlenArray, VarlenValuesBuffer};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use serde::{Deserialize, Serialize};
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
                input: &[DataTypeId::Utf8, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
            Signature {
                input: &[DataTypeId::LargeUtf8, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::LargeUtf8,
            },
        ]
    }
}

impl ScalarFunction for Repeat {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(RepeatUtf8Impl {
            large: PackedDecoder::new(state).decode_next()?,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Int64) => Ok(Box::new(RepeatUtf8Impl { large: false })),
            (DataType::LargeUtf8, DataType::Int64) => Ok(Box::new(RepeatUtf8Impl { large: true })),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepeatUtf8Impl {
    large: bool,
}

impl PlannedScalarFunction for RepeatUtf8Impl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Repeat
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.large)
    }

    fn return_type(&self) -> DataType {
        if self.large {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        }
    }

    fn execute(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let strings = arrays[0];
        let nums = arrays[1];
        Ok(match (strings.as_ref(), nums.as_ref()) {
            (Array2::Utf8(strings), Array2::Int64(nums)) => {
                let mut buffer = VarlenValuesBuffer::default();
                let validity = BinaryExecutor::execute(
                    strings,
                    nums,
                    |s, count| s.repeat(count as usize),
                    &mut buffer,
                )?;
                Array2::Utf8(VarlenArray::new(buffer, validity))
            }
            (Array2::LargeUtf8(strings), Array2::Int64(nums)) => {
                let mut buffer = VarlenValuesBuffer::default();
                let validity = BinaryExecutor::execute(
                    strings,
                    nums,
                    |s, count| s.repeat(count as usize),
                    &mut buffer,
                )?;
                Array2::LargeUtf8(VarlenArray::new(buffer, validity))
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}
