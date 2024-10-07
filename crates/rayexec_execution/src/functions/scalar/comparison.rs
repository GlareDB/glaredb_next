use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::{Array, Array2, BooleanArray, BooleanValuesBuffer};
use rayexec_bullet::compute::cast::array::{
    cast_decimal_to_new_precision_and_scale2, decimal_rescale,
};
use rayexec_bullet::compute::cast::behavior::CastFailBehavior;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalBinary, PhysicalF32, PhysicalF64, PhysicalI128, PhysicalI16, PhysicalI32, PhysicalI64,
    PhysicalI8, PhysicalInterval, PhysicalType, PhysicalU128, PhysicalU16, PhysicalU32,
    PhysicalU64, PhysicalU8, PhysicalUtf8,
};
use rayexec_bullet::executor::scalar::{BinaryExecutor, BinaryExecutor2};
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_error::{not_implemented, RayexecError, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;

// TODOs:
//
// - Normalize scales for decimals for comparisons (will be needed elsewhere too).
// - Normalize intervals for comparisons

const COMPARISON_SIGNATURES: &[Signature] = &[
    Signature {
        input: &[DataTypeId::Boolean, DataTypeId::Boolean],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int8, DataTypeId::Int8],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int16, DataTypeId::Int16],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int32, DataTypeId::Int32],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int64, DataTypeId::Int64],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt8, DataTypeId::UInt8],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt16, DataTypeId::UInt16],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt32, DataTypeId::UInt32],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt64, DataTypeId::UInt64],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Float32, DataTypeId::Float32],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Float64, DataTypeId::Float64],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Decimal64, DataTypeId::Decimal64],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Decimal128, DataTypeId::Decimal128],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Timestamp, DataTypeId::Timestamp],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Date32, DataTypeId::Date32],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Utf8, DataTypeId::Utf8],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::LargeUtf8, DataTypeId::LargeUtf8],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Binary, DataTypeId::Binary],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::LargeBinary, DataTypeId::LargeBinary],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
];

/// Describes a comparison betweeen a left and right element.
trait ComparisonOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EqOperation;

impl ComparisonOperation for EqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left == right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NotEqOperation;

impl ComparisonOperation for NotEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left != right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LtOperation;

impl ComparisonOperation for LtOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left < right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LtEqOperation;

impl ComparisonOperation for LtEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left <= right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GtOperation;

impl ComparisonOperation for GtOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left > right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GtEqOperation;

impl ComparisonOperation for GtEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left >= right
    }
}

fn execute<O: ComparisonOperation>(left: &Array, right: &Array) -> Result<Array> {
    let builder = ArrayBuilder {
        datatype: DataType::Boolean,
        buffer: BooleanBuffer::with_len(left.logical_len()),
    };

    // Decimal special cases.
    match (left.datatype(), right.datatype()) {
        (DataType::Decimal64(a), DataType::Decimal64(b)) => match a.scale.cmp(&b.scale) {
            Ordering::Greater => {
                let scaled_right = decimal_rescale::<PhysicalI64, Decimal64Type>(
                    right,
                    left.datatype().clone(),
                    CastFailBehavior::Error,
                )?;

                return BinaryExecutor::execute::<PhysicalI64, PhysicalI64, _, _>(
                    left,
                    &scaled_right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                );
            }
            Ordering::Less => {
                let scaled_left = decimal_rescale::<PhysicalI64, Decimal64Type>(
                    left,
                    right.datatype().clone(),
                    CastFailBehavior::Error,
                )?;

                return BinaryExecutor::execute::<PhysicalI64, PhysicalI64, _, _>(
                    &scaled_left,
                    right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                );
            }
            Ordering::Equal => {
                return BinaryExecutor::execute::<PhysicalI64, PhysicalI64, _, _>(
                    left,
                    right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                )
            }
        },
        (DataType::Decimal128(a), DataType::Decimal128(b)) => match a.scale.cmp(&b.scale) {
            Ordering::Greater => {
                let scaled_right = decimal_rescale::<PhysicalI128, Decimal128Type>(
                    right,
                    left.datatype().clone(),
                    CastFailBehavior::Error,
                )?;

                return BinaryExecutor::execute::<PhysicalI128, PhysicalI128, _, _>(
                    left,
                    &scaled_right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                );
            }
            Ordering::Less => {
                let scaled_left = decimal_rescale::<PhysicalI128, Decimal128Type>(
                    left,
                    right.datatype().clone(),
                    CastFailBehavior::Error,
                )?;

                return BinaryExecutor::execute::<PhysicalI128, PhysicalI128, _, _>(
                    &scaled_left,
                    right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                );
            }
            Ordering::Equal => {
                return BinaryExecutor::execute::<PhysicalI128, PhysicalI128, _, _>(
                    left,
                    right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                )
            }
        },

        _ => (), // Continue on.
    }

    match (
        left.array_data().physical_type(),
        right.array_data().physical_type(),
    ) {
        (PhysicalType::UntypedNull, PhysicalType::UntypedNull) => Err(RayexecError::new(
            "Generic binary operation on untyped null not supported",
        )),
        (PhysicalType::Int8, PhysicalType::Int8) => {
            BinaryExecutor::execute::<PhysicalI8, PhysicalI8, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Int16, PhysicalType::Int16) => {
            BinaryExecutor::execute::<PhysicalI16, PhysicalI16, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Int32, PhysicalType::Int32) => {
            BinaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Int64, PhysicalType::Int64) => {
            BinaryExecutor::execute::<PhysicalI64, PhysicalI64, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Int128, PhysicalType::Int128) => {
            BinaryExecutor::execute::<PhysicalI128, PhysicalI128, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }

        (PhysicalType::UInt8, PhysicalType::UInt8) => {
            BinaryExecutor::execute::<PhysicalU8, PhysicalU8, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::UInt16, PhysicalType::UInt16) => {
            BinaryExecutor::execute::<PhysicalU16, PhysicalU16, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::UInt32, PhysicalType::UInt32) => {
            BinaryExecutor::execute::<PhysicalU32, PhysicalU32, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::UInt64, PhysicalType::UInt64) => {
            BinaryExecutor::execute::<PhysicalU64, PhysicalU64, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::UInt128, PhysicalType::UInt128) => {
            BinaryExecutor::execute::<PhysicalU128, PhysicalU128, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Float32, PhysicalType::Float32) => {
            BinaryExecutor::execute::<PhysicalF32, PhysicalF32, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Float64, PhysicalType::Float64) => {
            BinaryExecutor::execute::<PhysicalF64, PhysicalF64, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Interval, PhysicalType::Interval) => {
            BinaryExecutor::execute::<PhysicalInterval, PhysicalInterval, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Utf8, PhysicalType::Utf8) => {
            BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (PhysicalType::Binary, PhysicalType::Binary) => {
            BinaryExecutor::execute::<PhysicalBinary, PhysicalBinary, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            )
        }
        (a, b) => Err(RayexecError::new(format!(
            "Unhandled physical types for generic binary operation: {a:?}, {b:?}"
        ))),
    }
}

fn execute2<O: ComparisonOperation>(left: &Array2, right: &Array2) -> Result<BooleanArray> {
    let mut buffer = BooleanValuesBuffer::with_capacity(left.len());
    let validity = match (left, right) {
        (Array2::Boolean(left), Array2::Boolean(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Int8(left), Array2::Int8(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Int16(left), Array2::Int16(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Int32(left), Array2::Int32(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Int64(left), Array2::Int64(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::UInt8(left), Array2::UInt8(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::UInt16(left), Array2::UInt16(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::UInt32(left), Array2::UInt32(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::UInt64(left), Array2::UInt64(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Float32(left), Array2::Float32(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Float64(left), Array2::Float64(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Decimal64(left), Array2::Decimal64(right)) => {
            match left.scale().cmp(&right.scale()) {
                Ordering::Greater => {
                    let scaled_right = cast_decimal_to_new_precision_and_scale2::<Decimal64Type>(
                        right,
                        Decimal64Type::MAX_PRECISION,
                        left.scale(),
                    )?;

                    BinaryExecutor2::execute(
                        left.get_primitive(),
                        scaled_right.get_primitive(),
                        O::compare,
                        &mut buffer,
                    )?
                }
                Ordering::Less => {
                    let scaled_left = cast_decimal_to_new_precision_and_scale2::<Decimal64Type>(
                        left,
                        Decimal64Type::MAX_PRECISION,
                        right.scale(),
                    )?;

                    BinaryExecutor2::execute(
                        scaled_left.get_primitive(),
                        right.get_primitive(),
                        O::compare,
                        &mut buffer,
                    )?
                }
                Ordering::Equal => BinaryExecutor2::execute(
                    left.get_primitive(),
                    right.get_primitive(),
                    O::compare,
                    &mut buffer,
                )?,
            }
        }
        (Array2::Decimal128(left), Array2::Decimal128(right)) => {
            match left.scale().cmp(&right.scale()) {
                Ordering::Greater => {
                    let scaled_right = cast_decimal_to_new_precision_and_scale2::<Decimal128Type>(
                        right,
                        Decimal128Type::MAX_PRECISION,
                        left.scale(),
                    )?;

                    BinaryExecutor2::execute(
                        left.get_primitive(),
                        scaled_right.get_primitive(),
                        O::compare,
                        &mut buffer,
                    )?
                }

                Ordering::Less => {
                    let scaled_left = cast_decimal_to_new_precision_and_scale2::<Decimal128Type>(
                        left,
                        Decimal128Type::MAX_PRECISION,
                        right.scale(),
                    )?;

                    BinaryExecutor2::execute(
                        scaled_left.get_primitive(),
                        right.get_primitive(),
                        O::compare,
                        &mut buffer,
                    )?
                }

                Ordering::Equal => BinaryExecutor2::execute(
                    left.get_primitive(),
                    right.get_primitive(),
                    O::compare,
                    &mut buffer,
                )?,
            }
        }
        (Array2::Timestamp(left), Array2::Timestamp(right)) => {
            // TODO: Unit check
            BinaryExecutor2::execute(
                left.get_primitive(),
                right.get_primitive(),
                O::compare,
                &mut buffer,
            )?
        }
        (Array2::Date32(left), Array2::Date32(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Date64(left), Array2::Date64(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Utf8(left), Array2::Utf8(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::LargeUtf8(left), Array2::LargeUtf8(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Binary(left), Array2::Binary(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::LargeBinary(left), Array2::LargeBinary(right)) => {
            BinaryExecutor2::execute(left, right, O::compare, &mut buffer)?
        }
        (left, right) => not_implemented!(
            "comparison between {} and {}",
            left.datatype(),
            right.datatype()
        ),
    };

    Ok(BooleanArray::new(buffer, validity))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Eq;

impl FunctionInfo for Eq {
    fn name(&self) -> &'static str {
        "="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for Eq {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(EqImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Timestamp(_), DataType::Timestamp(_))
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(EqImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EqImpl;

impl PlannedScalarFunction for EqImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Eq
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        execute::<EqOperation>(inputs[0], inputs[1])
    }

    fn execute2(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute2::<EqOperation>(left, right).map(Array2::Boolean)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Neq;

impl FunctionInfo for Neq {
    fn name(&self) -> &'static str {
        "<>"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["!="]
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for Neq {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(NeqImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Timestamp(_), DataType::Timestamp(_))
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(NeqImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeqImpl;

impl PlannedScalarFunction for NeqImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Neq
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        execute::<NotEqOperation>(inputs[0], inputs[1])
    }

    fn execute2(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute2::<NotEqOperation>(left, right).map(Array2::Boolean)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lt;

impl FunctionInfo for Lt {
    fn name(&self) -> &'static str {
        "<"
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for Lt {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(LtImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Timestamp(_), DataType::Timestamp(_))
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(LtImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LtImpl;

impl PlannedScalarFunction for LtImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Lt
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        execute::<LtOperation>(inputs[0], inputs[1])
    }

    fn execute2(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute2::<LtOperation>(left, right).map(Array2::Boolean)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtEq;

impl FunctionInfo for LtEq {
    fn name(&self) -> &'static str {
        "<="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for LtEq {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(LtEqImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Timestamp(_), DataType::Timestamp(_))
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(LtEqImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LtEqImpl;

impl PlannedScalarFunction for LtEqImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &LtEq
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        execute::<LtEqOperation>(inputs[0], inputs[1])
    }

    fn execute2(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute2::<LtEqOperation>(left, right).map(Array2::Boolean)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gt;

impl FunctionInfo for Gt {
    fn name(&self) -> &'static str {
        ">"
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for Gt {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(GtImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Timestamp(_), DataType::Timestamp(_))
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(GtImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GtImpl;

impl PlannedScalarFunction for GtImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Gt
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        execute::<GtOperation>(inputs[0], inputs[1])
    }

    fn execute2(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute2::<GtOperation>(left, right).map(Array2::Boolean)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtEq;

impl FunctionInfo for GtEq {
    fn name(&self) -> &'static str {
        ">="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for GtEq {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(GtEqImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Timestamp(_), DataType::Timestamp(_))
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(GtEqImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GtEqImpl;

impl PlannedScalarFunction for GtEqImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &GtEq
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        execute::<GtEqOperation>(inputs[0], inputs[1])
    }

    fn execute2(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute2::<GtEqOperation>(left, right).map(Array2::Boolean)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn eq_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

        let specialized = Eq
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([false, true, false]);

        assert_eq!(expected, out);
    }

    #[test]
    fn neq_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

        let specialized = Neq
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([true, false, true]);

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

        let specialized = Lt
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([true, false, true]);

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_eq_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

        let specialized = LtEq
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([true, true, true]);

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

        let specialized = Gt
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([false, false, false]);

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_eq_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

        let specialized = GtEq
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([false, true, false]);

        assert_eq!(expected, out);
    }
}
