use std::{ops::Deref, sync::Arc};

use rayexec_bullet::{
    array::{Array, ListArray, NullArray},
    compute::interleave::interleave,
    datatype::{DataType, DataTypeId},
    field::TypeSchema,
};
use rayexec_error::{RayexecError, Result};

use crate::{
    functions::{plan_check_num_args, FunctionInfo, Signature},
    logical::{consteval::ConstEval, expr::LogicalExpression},
};

use super::{PlannedScalarFunction, ScalarFunction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListExtract;

impl FunctionInfo for ListExtract {
    fn name(&self) -> &'static str {
        "list_extract"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::List, DataTypeId::Int64],
            variadic: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl ScalarFunction for ListExtract {
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        Err(RayexecError::new("Constant arguments required"))
    }

    fn plan_from_expressions(
        &self,
        inputs: &[&LogicalExpression],
        operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(operator_schema, &[]))
            .collect::<Result<Vec<_>>>()?;

        plan_check_num_args(self, &datatypes, 2)?;

        let index = ConstEval::default()
            .fold(inputs[1].clone())?
            .try_unwrap_constant()?
            .try_as_i64()?;

        let inner_datatype = match &datatypes[0] {
            DataType::List(meta) => meta.datatype.deref().clone(),
            other => {
                return Err(RayexecError::new(format!(
                    "Cannot index into non-list type, got {other}",
                )))
            }
        };

        Ok(Box::new(ListExtractImpl {
            datatype: inner_datatype,
            index: index as usize,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListExtractImpl {
    datatype: DataType,
    index: usize,
}

impl PlannedScalarFunction for ListExtractImpl {
    fn name(&self) -> &'static str {
        "list_extract_impl"
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListValues;

impl FunctionInfo for ListValues {
    fn name(&self) -> &'static str {
        "list_values"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
        }]
    }
}

impl ScalarFunction for ListValues {
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        let first = match inputs.first() {
            Some(dt) => dt,
            None => {
                return Ok(Box::new(ListValuesImpl {
                    datatype: DataType::Null,
                }))
            }
        };

        for dt in inputs {
            // TODO: It would be ideal to have the planner add a cast where
            // needed, but it's probably more straightforward to cast in the
            // implemenation if these don't match.
            if dt != first {
                return Err(RayexecError::new(format!(
                    "Not all inputs are the same type, got {dt}, expected {first}"
                )));
            }
        }

        Ok(Box::new(ListValuesImpl {
            datatype: first.clone(),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListValuesImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for ListValuesImpl {
    fn name(&self) -> &'static str {
        "list_values_impl"
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        let len = match inputs.first() {
            Some(arr) => arr.len(),
            None => {
                return Ok(Array::List(ListArray::new(
                    Array::Null(NullArray::new(0)),
                    vec![0],
                    None,
                )))
            }
        };

        let mut indices = Vec::with_capacity(len * inputs.len());
        for row_idx in 0..len {
            for arr_idx in 0..inputs.len() {
                indices.push((arr_idx, row_idx));
            }
        }

        let refs: Vec<_> = inputs.iter().map(|a| a.as_ref()).collect(); // TODO: Update interleave to accept arc refs
        let child = interleave(&refs, &indices)?;

        let mut offsets = Vec::with_capacity(len);
        let mut offset = 0;
        for _ in 0..len {
            offsets.push(offset);
            offset += inputs.len() as i32;
        }
        offsets.push(offset);

        // TODO: How do we want to handle validity here? If one of the inputs is
        // a null array, mark it as invalid?

        Ok(Array::List(ListArray::new(child, offsets, None)))
    }
}
