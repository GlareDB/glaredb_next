use crate::functions::scalar::macros::primitive_unary_execute;
use crate::functions::{
    invalid_input_types_error, plan_check_num_args, unhandled_physical_types_err, FunctionInfo,
    Signature,
};
use rayexec_bullet::array::{Array, Array2};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF32, PhysicalF64, PhysicalI128, PhysicalI16, PhysicalI32, PhysicalI64, PhysicalI8,
    PhysicalStorage, PhysicalType, PhysicalU128, PhysicalU16, PhysicalU32, PhysicalU64, PhysicalU8,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::macros::primitive_unary_execute_bool;
use super::{PlannedScalarFunction, ScalarFunction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Negate;

impl FunctionInfo for Negate {
    fn name(&self) -> &'static str {
        "negate"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float32],
                variadic: None,
                return_type: DataTypeId::Float32,
            },
            Signature {
                input: &[DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Int8],
                variadic: None,
                return_type: DataTypeId::Int8,
            },
            Signature {
                input: &[DataTypeId::Int16],
                variadic: None,
                return_type: DataTypeId::Int16,
            },
            Signature {
                input: &[DataTypeId::Int32],
                variadic: None,
                return_type: DataTypeId::Int32,
            },
            Signature {
                input: &[DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
            Signature {
                input: &[DataTypeId::Interval],
                variadic: None,
                return_type: DataTypeId::Interval,
            },
        ]
    }
}

impl ScalarFunction for Negate {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(NegateImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => Ok(Box::new(NegateImpl {
                datatype: inputs[0].clone(),
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NegateImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for NegateImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Negate
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute2(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let first = arrays[0];
        Ok(match first.as_ref() {
            Array2::Int8(input) => {
                primitive_unary_execute!(input, Int8, |a| -a)
            }
            Array2::Int16(input) => {
                primitive_unary_execute!(input, Int16, |a| -a)
            }
            Array2::Int32(input) => {
                primitive_unary_execute!(input, Int32, |a| -a)
            }
            Array2::Int64(input) => {
                primitive_unary_execute!(input, Int64, |a| -a)
            }
            Array2::Float32(input) => {
                primitive_unary_execute!(input, Float32, |a| -a)
            }
            Array2::Float64(input) => {
                primitive_unary_execute!(input, Float64, |a| -a)
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let a = inputs[0];

        let datatype = self.datatype.clone();

        match a.physical_type() {
            PhysicalType::Int8 => UnaryExecutor::execute::<PhysicalI8, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Int16 => UnaryExecutor::execute::<PhysicalI16, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Int32 => UnaryExecutor::execute::<PhysicalI32, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Int64 => UnaryExecutor::execute::<PhysicalI64, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Int128 => UnaryExecutor::execute::<PhysicalI128, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Float32 => UnaryExecutor::execute::<PhysicalF32, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Float64 => UnaryExecutor::execute::<PhysicalF64, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            other => Err(unhandled_physical_types_err(self, [other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Not;

impl FunctionInfo for Not {
    fn name(&self) -> &'static str {
        "not"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Boolean],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for Not {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(NotImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Boolean => Ok(Box::new(NotImpl)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotImpl;

impl PlannedScalarFunction for NotImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Not
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute2(&self, inputs: &[&Arc<Array2>]) -> Result<Array2> {
        Ok(match inputs[0].as_ref() {
            Array2::Boolean(arr) => primitive_unary_execute_bool!(arr, |b| !b),
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}
