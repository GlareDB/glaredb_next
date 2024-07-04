use std::{ops::Deref, sync::Arc};

use rayexec_bullet::{
    array::{Array, ListArray, OffsetIndex, PrimitiveArray},
    bitmap::Bitmap,
    datatype::{DataType, DataTypeId, ListTypeMeta},
    field::TypeSchema,
};
use rayexec_error::{RayexecError, Result};

use crate::{
    functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature},
    logical::{consteval::ConstEval, expr::LogicalExpression},
};

use super::{PlannedScalarFunction, ScalarFunction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Concat;

impl FunctionInfo for Concat {
    fn name(&self) -> &'static str {
        "concat"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: Some(DataTypeId::Utf8),
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for Concat {
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        for input in inputs {
            if input.datatype_id() != DataTypeId::Utf8 {
                return Err(invalid_input_types_error(self, inputs));
            }
        }

        Ok(Box::new(StringConcatImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringConcatImpl;

impl PlannedScalarFunction for StringConcatImpl {
    fn name(&self) -> &'static str {
        "string_concat_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        unimplemented!()
    }
}
