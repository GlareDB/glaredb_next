use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::{Array2, BooleanArray, BooleanValuesBuffer};
use rayexec_bullet::compute::cast::array::cast_decimal_to_new_precision_and_scale;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_error::{not_implemented, Result};
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

fn execute<O: ComparisonOperation>(left: &Array2, right: &Array2) -> Result<BooleanArray> {
    let mut buffer = BooleanValuesBuffer::with_capacity(left.len());
    let validity = match (left, right) {
        (Array2::Boolean(left), Array2::Boolean(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Int8(left), Array2::Int8(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Int16(left), Array2::Int16(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Int32(left), Array2::Int32(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Int64(left), Array2::Int64(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::UInt8(left), Array2::UInt8(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::UInt16(left), Array2::UInt16(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::UInt32(left), Array2::UInt32(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::UInt64(left), Array2::UInt64(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Float32(left), Array2::Float32(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Float64(left), Array2::Float64(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Decimal64(left), Array2::Decimal64(right)) => {
            match left.scale().cmp(&right.scale()) {
                Ordering::Greater => {
                    let scaled_right = cast_decimal_to_new_precision_and_scale::<Decimal64Type>(
                        right,
                        Decimal64Type::MAX_PRECISION,
                        left.scale(),
                    )?;

                    BinaryExecutor::execute(
                        left.get_primitive(),
                        scaled_right.get_primitive(),
                        O::compare,
                        &mut buffer,
                    )?
                }
                Ordering::Less => {
                    let scaled_left = cast_decimal_to_new_precision_and_scale::<Decimal64Type>(
                        left,
                        Decimal64Type::MAX_PRECISION,
                        right.scale(),
                    )?;

                    BinaryExecutor::execute(
                        scaled_left.get_primitive(),
                        right.get_primitive(),
                        O::compare,
                        &mut buffer,
                    )?
                }
                Ordering::Equal => BinaryExecutor::execute(
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
                    let scaled_right = cast_decimal_to_new_precision_and_scale::<Decimal128Type>(
                        right,
                        Decimal128Type::MAX_PRECISION,
                        left.scale(),
                    )?;

                    BinaryExecutor::execute(
                        left.get_primitive(),
                        scaled_right.get_primitive(),
                        O::compare,
                        &mut buffer,
                    )?
                }

                Ordering::Less => {
                    let scaled_left = cast_decimal_to_new_precision_and_scale::<Decimal128Type>(
                        left,
                        Decimal128Type::MAX_PRECISION,
                        right.scale(),
                    )?;

                    BinaryExecutor::execute(
                        scaled_left.get_primitive(),
                        right.get_primitive(),
                        O::compare,
                        &mut buffer,
                    )?
                }

                Ordering::Equal => BinaryExecutor::execute(
                    left.get_primitive(),
                    right.get_primitive(),
                    O::compare,
                    &mut buffer,
                )?,
            }
        }
        (Array2::Timestamp(left), Array2::Timestamp(right)) => {
            // TODO: Unit check
            BinaryExecutor::execute(
                left.get_primitive(),
                right.get_primitive(),
                O::compare,
                &mut buffer,
            )?
        }
        (Array2::Date32(left), Array2::Date32(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Date64(left), Array2::Date64(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Utf8(left), Array2::Utf8(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::LargeUtf8(left), Array2::LargeUtf8(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::Binary(left), Array2::Binary(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
        }
        (Array2::LargeBinary(left), Array2::LargeBinary(right)) => {
            BinaryExecutor::execute(left, right, O::compare, &mut buffer)?
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

    fn execute(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute::<EqOperation>(left, right).map(Array2::Boolean)
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

    fn execute(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute::<NotEqOperation>(left, right).map(Array2::Boolean)
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

    fn execute(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute::<LtOperation>(left, right).map(Array2::Boolean)
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

    fn execute(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute::<LtEqOperation>(left, right).map(Array2::Boolean)
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

    fn execute(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute::<GtOperation>(left, right).map(Array2::Boolean)
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

    fn execute(&self, arrays: &[&Arc<Array2>]) -> Result<Array2> {
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        execute::<GtEqOperation>(left, right).map(Array2::Boolean)
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::{BooleanArray, Int32Array};

    use super::*;

    #[test]
    fn eq_i32() {
        let a = Arc::new(Array2::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array2::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Eq
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array2::Boolean(BooleanArray::from_iter([false, true, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn neq_i32() {
        let a = Arc::new(Array2::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array2::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Neq
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array2::Boolean(BooleanArray::from_iter([true, false, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_i32() {
        let a = Arc::new(Array2::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array2::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Lt
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array2::Boolean(BooleanArray::from_iter([true, false, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_eq_i32() {
        let a = Arc::new(Array2::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array2::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = LtEq
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array2::Boolean(BooleanArray::from_iter([true, true, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_i32() {
        let a = Arc::new(Array2::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array2::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Gt
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array2::Boolean(BooleanArray::from_iter([false, false, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_eq_i32() {
        let a = Arc::new(Array2::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array2::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = GtEq
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array2::Boolean(BooleanArray::from_iter([false, true, false]));

        assert_eq!(expected, out);
    }
}
