use super::{GenericScalarFunction, ScalarFn, SpecializedScalarFunction};
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, InputTypes, ReturnType,
    Signature,
};
use rayexec_bullet::array::{BooleanArray, BooleanValuesBuffer};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct And;

impl FunctionInfo for And {
    fn name(&self) -> &'static str {
        "and"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: InputTypes::Exact(&[DataType::Boolean, DataType::Boolean]),
            return_type: ReturnType::Static(DataType::Boolean),
        }]
    }
}

impl GenericScalarFunction for And {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean) => Ok(Box::new(AndBool)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AndBool;

impl SpecializedScalarFunction for AndBool {
    fn function_impl(&self) -> ScalarFn {
        fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Boolean(first), Array::Boolean(second)) => {
                    let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
                    let validity =
                        BinaryExecutor::execute(first, second, |a, b| a && b, &mut buffer)?;
                    Array::Boolean(BooleanArray::new(buffer, validity))
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Or;

impl FunctionInfo for Or {
    fn name(&self) -> &'static str {
        "or"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: InputTypes::Exact(&[DataType::Boolean, DataType::Boolean]),
            return_type: ReturnType::Static(DataType::Boolean),
        }]
    }
}

impl GenericScalarFunction for Or {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean) => Ok(Box::new(OrBool)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrBool;

impl SpecializedScalarFunction for OrBool {
    fn function_impl(&self) -> ScalarFn {
        fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
            let first = arrays[0];
            let second = arrays[1];
            Ok(match (first.as_ref(), second.as_ref()) {
                (Array::Boolean(first), Array::Boolean(second)) => {
                    let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
                    let validity =
                        BinaryExecutor::execute(first, second, |a, b| a || b, &mut buffer)?;
                    Array::Boolean(BooleanArray::new(buffer, validity))
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        inner
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::BooleanArray;

    use super::*;

    #[test]
    fn and_bool() {
        let a = Arc::new(Array::Boolean(BooleanArray::from_iter([
            true, false, false,
        ])));
        let b = Arc::new(Array::Boolean(BooleanArray::from_iter([true, true, false])));

        let specialized = And
            .specialize(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, false, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn or_bool() {
        let a = Arc::new(Array::Boolean(BooleanArray::from_iter([
            true, false, false,
        ])));
        let b = Arc::new(Array::Boolean(BooleanArray::from_iter([true, true, false])));

        let specialized = Or
            .specialize(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, true, false]));

        assert_eq!(expected, out);
    }
}
