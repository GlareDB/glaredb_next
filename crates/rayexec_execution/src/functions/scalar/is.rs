use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::PhysicalAny;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;

use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNull;

impl FunctionInfo for IsNull {
    fn name(&self) -> &'static str {
        "is_null"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for IsNull {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(CheckNullImpl::<true>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(CheckNullImpl::<true>))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotNull;

impl FunctionInfo for IsNotNull {
    fn name(&self) -> &'static str {
        "is_not_null"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for IsNotNull {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(CheckNullImpl::<false>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(CheckNullImpl::<false>))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckNullImpl<const IS_NULL: bool>;

impl<const IS_NULL: bool> PlannedScalarFunction for CheckNullImpl<IS_NULL> {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        if IS_NULL {
            &IsNull
        } else {
            &IsNotNull
        }
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];

        let (initial, updated) = if IS_NULL {
            // Executor will only execute on non-null inputs, so we can assume
            // everything is null first then selectively set false for things
            // that the executor executes.
            (true, false)
        } else {
            (false, true)
        };

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len_and_default_value(input.logical_len(), initial),
        };
        let array = UnaryExecutor::execute::<PhysicalAny, _, _>(input, builder, |_, buf| {
            buf.put(&updated)
        })?;

        // Drop validity.
        let data = array.into_array_data();
        Ok(Array::new_with_array_data(DataType::Boolean, data))
    }
}
