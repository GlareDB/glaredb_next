use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::{Array, Array2};
use rayexec_bullet::array::{VarlenArray, VarlenValuesBuffer};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::{PhysicalI64, PhysicalUtf8};
use rayexec_bullet::executor::scalar::{BinaryExecutor, BinaryExecutor2};
use rayexec_error::Result;
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
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(RepeatUtf8Impl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Int64) => Ok(Box::new(RepeatUtf8Impl)),
            (DataType::LargeUtf8, DataType::Int64) => Ok(Box::new(RepeatUtf8Impl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepeatUtf8Impl;

impl PlannedScalarFunction for RepeatUtf8Impl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Repeat
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute2(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let strings = arrays[0];
        let nums = arrays[1];
        Ok(match (strings.as_ref(), nums.as_ref()) {
            (Array2::Utf8(strings), Array2::Int64(nums)) => {
                let mut buffer = VarlenValuesBuffer::default();
                let validity = BinaryExecutor2::execute(
                    strings,
                    nums,
                    |s, count| s.repeat(count as usize),
                    &mut buffer,
                )?;
                Array2::Utf8(VarlenArray::new(buffer, validity))
            }
            (Array2::LargeUtf8(strings), Array2::Int64(nums)) => {
                let mut buffer = VarlenValuesBuffer::default();
                let validity = BinaryExecutor2::execute(
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

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let strings = inputs[0];
        let nums = inputs[1];

        // TODO: Capacity

        let mut string_buf = String::new();

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, _, _>(
            strings,
            nums,
            ArrayBuilder {
                datatype: DataType::Utf8,
                buffer: GermanVarlenBuffer::with_len(strings.logical_len()),
            },
            |s, num, buf| {
                string_buf.clear();
                for _ in 0..num {
                    string_buf.push_str(s);
                }
                buf.put(string_buf.as_str())
            },
        )
    }
}
