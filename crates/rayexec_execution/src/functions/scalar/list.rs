use std::{ops::Deref, sync::Arc};

use rayexec_bullet::{
    array::Array,
    datatype::{DataType, DataTypeId},
    field::TypeSchema,
};
use rayexec_error::{RayexecError, Result};

use crate::{
    functions::{
        invalid_input_types_error, plan_check_num_args,
        scalar::macros::primitive_unary_execute_bool, FunctionInfo, Signature,
    },
    logical::{consteval::ConstEval, expr::LogicalExpression},
};

use super::{comparison::EqImpl, PlannedScalarFunction, ScalarFunction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListExtract;

impl FunctionInfo for ListExtract {
    fn name(&self) -> &'static str {
        "list_extract"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::List, DataTypeId::Int64],
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
            input: &[DataTypeId::List],
            return_type: DataTypeId::List,
        }]
    }
}

impl ScalarFunction for ListValues {
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unimplemented!()
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
        unimplemented!()
    }
}
