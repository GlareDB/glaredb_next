use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use rayexec_bullet::array::{Array, Array2};
use rayexec_bullet::array::{BooleanArray, BooleanValuesBuffer};
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::PhysicalBool;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UniformExecutor, UniformExecutor2};
use rayexec_bullet::storage::{BooleanStorage, PrimitiveStorage};
use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};
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
            input: &[],
            variadic: Some(DataTypeId::Boolean),
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for And {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(AndImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        for input in inputs {
            if input.datatype_id() != DataTypeId::Boolean {
                return Err(invalid_input_types_error(self, inputs));
            }
        }

        Ok(Box::new(AndImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AndImpl;

impl PlannedScalarFunction for AndImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &And
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute2(&self, inputs: &[&Arc<Array2>]) -> Result<Array2> {
        let first = match inputs.first() {
            Some(first) => first,
            None => return Ok(Array2::Boolean(BooleanArray::new_nulls(1))),
        };

        let bool_arrs = inputs
            .iter()
            .map(|arr| match arr.as_ref() {
                Array2::Boolean(arr) => Ok(arr),
                other => Err(RayexecError::new(format!(
                    "Expected Boolean arrays, got {}",
                    other.datatype(),
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
        let validity =
            UniformExecutor2::execute(&bool_arrs, |bools| bools.iter().all(|b| *b), &mut buffer)?;

        Ok(Array2::Boolean(BooleanArray::new(buffer, validity)))
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        match inputs.len() {
            0 => {
                let mut array = Array::new_with_array_data(
                    DataType::Boolean,
                    BooleanStorage::from(Bitmap::new_with_val(false, 1)),
                );
                array.set_physical_validity(0, false);
                Ok(array)
            }
            1 => Ok(inputs[0].clone()),
            2 => {
                let a = inputs[0];
                let b = inputs[1];
                BinaryExecutor::execute::<PhysicalBool, PhysicalBool, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a && b)),
                )
            }
            _ => {
                let len = inputs[0].logical_len();
                UniformExecutor::execute::<PhysicalBool, _, _>(
                    inputs,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(len),
                    },
                    |bools, buf| buf.put(&(bools.iter().all(|b| *b))),
                )
            }
        }
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
            input: &[],
            variadic: Some(DataTypeId::Boolean),
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for Or {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(OrImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        for input in inputs {
            if input.datatype_id() != DataTypeId::Boolean {
                return Err(invalid_input_types_error(self, inputs));
            }
        }

        Ok(Box::new(OrImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrImpl;

impl PlannedScalarFunction for OrImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Or
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute2(&self, inputs: &[&Arc<Array2>]) -> Result<Array2> {
        let first = match inputs.first() {
            Some(first) => first,
            None => return Ok(Array2::Boolean(BooleanArray::new_nulls(1))),
        };

        let bool_arrs = inputs
            .iter()
            .map(|arr| match arr.as_ref() {
                Array2::Boolean(arr) => Ok(arr),
                other => Err(RayexecError::new(format!(
                    "Expected Boolean arrays, got {}",
                    other.datatype(),
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
        let validity =
            UniformExecutor2::execute(&bool_arrs, |bools| bools.iter().any(|b| *b), &mut buffer)?;

        Ok(Array2::Boolean(BooleanArray::new(buffer, validity)))
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        match inputs.len() {
            0 => {
                let mut array = Array::new_with_array_data(
                    DataType::Boolean,
                    BooleanStorage::from(Bitmap::new_with_val(false, 1)),
                );
                array.set_physical_validity(0, false);
                Ok(array)
            }
            1 => Ok(inputs[0].clone()),
            2 => {
                let a = inputs[0];
                let b = inputs[1];
                BinaryExecutor::execute::<PhysicalBool, PhysicalBool, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a || b)),
                )
            }
            _ => {
                let len = inputs[0].logical_len();
                UniformExecutor::execute::<PhysicalBool, _, _>(
                    inputs,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(len),
                    },
                    |bools, buf| buf.put(&(bools.iter().any(|b| *b))),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::BooleanArray;

    use super::*;

    #[test]
    fn and_bool() {
        let a = Arc::new(Array2::Boolean(BooleanArray::from_iter([
            true, false, false,
        ])));
        let b = Arc::new(Array2::Boolean(BooleanArray::from_iter([
            true, true, false,
        ])));

        let specialized = And
            .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = specialized.execute2(&[&a, &b]).unwrap();
        let expected = Array2::Boolean(BooleanArray::from_iter([true, false, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn or_bool() {
        let a = Arc::new(Array2::Boolean(BooleanArray::from_iter([
            true, false, false,
        ])));
        let b = Arc::new(Array2::Boolean(BooleanArray::from_iter([
            true, true, false,
        ])));

        let specialized = Or
            .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = specialized.execute2(&[&a, &b]).unwrap();
        let expected = Array2::Boolean(BooleanArray::from_iter([true, true, false]));

        assert_eq!(expected, out);
    }
}
