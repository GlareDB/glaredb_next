use super::{PlannedScalarFunction, ScalarFunction, ScalarFunctionSet};
use crate::logical::operator::LogicalExpression;
use rayexec_bullet::array::Array;
use rayexec_bullet::array::{BooleanArray, BooleanValuesBuffer};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::field::TypeSchema;
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

pub fn and_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("and");
    set.add_function(
        &[DataTypeId::Boolean, DataTypeId::Boolean],
        Box::new(AndFunction),
    );
    set
}

pub fn or_function_set() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("or");
    set.add_function(
        &[DataTypeId::Boolean, DataTypeId::Boolean],
        Box::new(AndFunction),
    );
    set
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AndFunction;

impl ScalarFunction for AndFunction {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(AndImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AndImpl;

impl PlannedScalarFunction for AndImpl {
    fn name(&self) -> &'static str {
        "and_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        let first = inputs[0];
        let second = inputs[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
                let validity = BinaryExecutor::execute(first, second, |a, b| a && b, &mut buffer)?;
                Array::Boolean(BooleanArray::new(buffer, validity))
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrFunction;

impl ScalarFunction for OrFunction {
    fn plan(
        &self,
        _inputs: &[LogicalExpression],
        _operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(OrImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrImpl;

impl PlannedScalarFunction for OrImpl {
    fn name(&self) -> &'static str {
        "or_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        let first = inputs[0];
        let second = inputs[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
                let validity = BinaryExecutor::execute(first, second, |a, b| a || b, &mut buffer)?;
                Array::Boolean(BooleanArray::new(buffer, validity))
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[cfg(test)]
mod tests {
    // use rayexec_bullet::array::BooleanArray;

    // use super::*;

    // #[test]
    // fn and_bool() {
    //     let a = Arc::new(Array::Boolean(BooleanArray::from_iter([
    //         true, false, false,
    //     ])));
    //     let b = Arc::new(Array::Boolean(BooleanArray::from_iter([true, true, false])));

    //     let specialized = And
    //         .specialize(&[DataType::Boolean, DataType::Boolean])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::Boolean(BooleanArray::from_iter([true, false, false]));

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn or_bool() {
    //     let a = Arc::new(Array::Boolean(BooleanArray::from_iter([
    //         true, false, false,
    //     ])));
    //     let b = Arc::new(Array::Boolean(BooleanArray::from_iter([true, true, false])));

    //     let specialized = Or
    //         .specialize(&[DataType::Boolean, DataType::Boolean])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::Boolean(BooleanArray::from_iter([true, true, false]));

    //     assert_eq!(expected, out);
    // }
}
