use super::{
    GenericScalarFunction, PlannedScalarFunction, ScalarFunction, ScalarFunctionSet,
    SpecializedScalarFunction,
};
use crate::functions::scalar::macros::cmp_binary_execute;
use crate::logical::operator::LogicalExpression;
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::field::TypeSchema;
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

// TODOs:
//
// - Normalize scales for decimals for comparisons (will be needed elsewhere too).
// - Normalize intervals for comparisons

pub fn eq_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("=");
    add_cmp_functions(&mut set, Box::new(EqFunction));
    set
}

pub fn neq_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::with_aliases("!=", &["<>"]);
    add_cmp_functions(&mut set, Box::new(NeqFunction));
    set
}

pub fn lteq_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("<=");
    add_cmp_functions(&mut set, Box::new(LtEqFunction));
    set
}

pub fn lt_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("<");
    add_cmp_functions(&mut set, Box::new(LtFunction));
    set
}

pub fn gt_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new(">");
    add_cmp_functions(&mut set, Box::new(GtFunction));
    set
}

pub fn gteq_scalar_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new(">=");
    add_cmp_functions(&mut set, Box::new(GtEqFunction));
    set
}

#[rustfmt::skip]
fn add_cmp_functions(set: &mut ScalarFunctionSet, func: Box<dyn ScalarFunction>) {
    set.add_function(&[DataTypeId::Boolean, DataTypeId::Boolean], func.clone());
    set.add_function(&[DataTypeId::Int8, DataTypeId::Int8], func.clone());
    set.add_function(&[DataTypeId::Int16, DataTypeId::Int16], func.clone());
    set.add_function(&[DataTypeId::Int32, DataTypeId::Int32], func.clone());
    set.add_function(&[DataTypeId::Int64, DataTypeId::Int64], func.clone());
    set.add_function(&[DataTypeId::UInt8, DataTypeId::UInt8], func.clone());
    set.add_function(&[DataTypeId::UInt16, DataTypeId::UInt16], func.clone());
    set.add_function(&[DataTypeId::UInt32, DataTypeId::UInt32], func.clone());
    set.add_function(&[DataTypeId::UInt64, DataTypeId::UInt64], func.clone());
    set.add_function(&[DataTypeId::Float32, DataTypeId::Float32], func.clone());
    set.add_function(&[DataTypeId::Float64, DataTypeId::Float64], func.clone());
    set.add_function(&[DataTypeId::Decimal64, DataTypeId::Decimal64], func.clone());
    set.add_function(&[DataTypeId::Decimal128, DataTypeId::Decimal128], func.clone());
    set.add_function(&[DataTypeId::TimestampSeconds, DataTypeId::TimestampSeconds], func.clone());
    set.add_function(&[DataTypeId::TimestampMilliseconds, DataTypeId::TimestampMilliseconds], func.clone());
    set.add_function(&[DataTypeId::TimestampMicroseconds, DataTypeId::TimestampMicroseconds], func.clone());
    set.add_function(&[DataTypeId::TimestampNanoseconds, DataTypeId::TimestampNanoseconds], func.clone());
    set.add_function(&[DataTypeId::Date32, DataTypeId::Date32], func.clone());
    set.add_function(&[DataTypeId::Date64, DataTypeId::Date64], func.clone());
    set.add_function(&[DataTypeId::Utf8, DataTypeId::Utf8], func.clone());
    set.add_function(&[DataTypeId::LargeUtf8, DataTypeId::LargeUtf8], func.clone());
    set.add_function(&[DataTypeId::Binary, DataTypeId::Binary], func.clone());
    set.add_function(&[DataTypeId::LargeBinary, DataTypeId::LargeBinary], func.clone());
}

macro_rules! generate_cmp_function {
    ($name:ident, $op:expr) => {
        fn $name(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Boolean(first), Array::Boolean(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Int8(first), Array::Int8(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Int16(first), Array::Int16(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Int32(first), Array::Int32(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Int64(first), Array::Int64(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::UInt8(first), Array::UInt8(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::UInt16(first), Array::UInt16(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::UInt32(first), Array::UInt32(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::UInt64(first), Array::UInt64(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Float32(first), Array::Float32(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Float64(first), Array::Float64(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Decimal64(first), Array::Decimal64(second)) => {
                    // TODO: Scale check
                    cmp_binary_execute!(first.get_primitive(), second.get_primitive(), $op)
                }
                (Array::Decimal128(first), Array::Decimal128(second)) => {
                    // TODO: Scale check
                    cmp_binary_execute!(first.get_primitive(), second.get_primitive(), $op)
                }
                (Array::TimestampSeconds(first), Array::TimestampSeconds(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::TimestampMilliseconds(first), Array::TimestampMilliseconds(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::TimestampMicroseconds(first), Array::TimestampMicroseconds(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::TimestampNanoseconds(first), Array::TimestampNanoseconds(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Date32(first), Array::Date32(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Date64(first), Array::Date64(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Utf8(first), Array::Utf8(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::LargeUtf8(first), Array::LargeUtf8(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::Binary(first), Array::Binary(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                (Array::LargeBinary(first), Array::LargeBinary(second)) => {
                    cmp_binary_execute!(first, second, $op)
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }
    };
}

generate_cmp_function!(cmp_eq, |a, b| a == b);
generate_cmp_function!(cmp_neq, |a, b| a != b);
generate_cmp_function!(cmp_lt, |a, b| a < b);
generate_cmp_function!(cmp_lteq, |a, b| a <= b);
generate_cmp_function!(cmp_gt, |a, b| a > b);
generate_cmp_function!(cmp_gteq, |a, b| a >= b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EqFunction;

impl ScalarFunction for EqFunction {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(EqImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EqImpl;

impl PlannedScalarFunction for EqImpl {
    fn name(&self) -> &'static str {
        "eq_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        cmp_eq(arrays)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeqFunction;

impl ScalarFunction for NeqFunction {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(NeqImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeqImpl;

impl PlannedScalarFunction for NeqImpl {
    fn name(&self) -> &'static str {
        "neq_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        cmp_neq(arrays)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtFunction;

impl ScalarFunction for LtFunction {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(LtImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtImpl;

impl PlannedScalarFunction for LtImpl {
    fn name(&self) -> &'static str {
        "lt_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        cmp_lt(arrays)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtEqFunction;

impl ScalarFunction for LtEqFunction {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(LtEqImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtEqImpl;

impl PlannedScalarFunction for LtEqImpl {
    fn name(&self) -> &'static str {
        "lteq_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        cmp_lteq(arrays)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtFunction;

impl ScalarFunction for GtFunction {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(GtImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtImpl;

impl PlannedScalarFunction for GtImpl {
    fn name(&self) -> &'static str {
        "gt_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        cmp_gt(arrays)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtEqFunction;

impl ScalarFunction for GtEqFunction {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(GtEqImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtEqImpl;

impl PlannedScalarFunction for GtEqImpl {
    fn name(&self) -> &'static str {
        "gteq_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        cmp_gteq(arrays)
    }
}

// #[cfg(test)]
// mod tests {
//     use rayexec_bullet::array::{BooleanArray, Int32Array};

//     use super::*;

//     #[test]
//     fn eq_i32() {
//         let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
//         let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

//         let specialized = Eq.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

//         let out = specialized.execute(&[&a, &b]).unwrap();
//         let expected = Array::Boolean(BooleanArray::from_iter([false, true, false]));

//         assert_eq!(expected, out);
//     }

//     #[test]
//     fn neq_i32() {
//         let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
//         let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

//         let specialized = Neq.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

//         let out = specialized.execute(&[&a, &b]).unwrap();
//         let expected = Array::Boolean(BooleanArray::from_iter([true, false, true]));

//         assert_eq!(expected, out);
//     }

//     #[test]
//     fn lt_i32() {
//         let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
//         let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

//         let specialized = Lt.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

//         let out = specialized.execute(&[&a, &b]).unwrap();
//         let expected = Array::Boolean(BooleanArray::from_iter([true, false, true]));

//         assert_eq!(expected, out);
//     }

//     #[test]
//     fn lt_eq_i32() {
//         let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
//         let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

//         let specialized = LtEq
//             .specialize(&[DataType::Int32, DataType::Int32])
//             .unwrap();

//         let out = specialized.execute(&[&a, &b]).unwrap();
//         let expected = Array::Boolean(BooleanArray::from_iter([true, true, true]));

//         assert_eq!(expected, out);
//     }

//     #[test]
//     fn gt_i32() {
//         let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
//         let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

//         let specialized = Gt.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

//         let out = specialized.execute(&[&a, &b]).unwrap();
//         let expected = Array::Boolean(BooleanArray::from_iter([false, false, false]));

//         assert_eq!(expected, out);
//     }

//     #[test]
//     fn gt_eq_i32() {
//         let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
//         let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

//         let specialized = GtEq
//             .specialize(&[DataType::Int32, DataType::Int32])
//             .unwrap();

//         let out = specialized.execute(&[&a, &b]).unwrap();
//         let expected = Array::Boolean(BooleanArray::from_iter([false, true, false]));

//         assert_eq!(expected, out);
//     }
// }
