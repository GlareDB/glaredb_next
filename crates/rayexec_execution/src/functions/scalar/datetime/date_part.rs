use std::sync::Arc;

use rayexec_bullet::{
    array::Array,
    compute::date::{self, ExtractDatePart},
    datatype::{DataType, DataTypeId, DecimalTypeMeta},
    scalar::decimal::{Decimal64Type, DecimalType},
};
use rayexec_error::{not_implemented, Result};

use crate::{
    expr::Expression,
    functions::{
        exec_invalid_array_type_err, invalid_input_types_error, plan_check_num_args,
        scalar::{PlannedScalarFunction, ScalarFunction},
        FunctionInfo, Signature,
    },
    logical::{binder::bind_context::BindContext, consteval::ConstEval},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatePart;

impl FunctionInfo for DatePart {
    fn name(&self) -> &'static str {
        "date_part"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Date32],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Date64],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Timestamp],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
        ]
    }
}

impl ScalarFunction for DatePart {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        not_implemented!("decoding date_part")
    }

    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn plan_from_expressions(
        &self,
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        // TODO: 3rd arg for optional timezone
        plan_check_num_args(self, &datatypes, 2)?;

        // Requires first argument to be constant.
        let mut part = ConstEval::default()
            .fold(inputs[0].clone())?
            .try_unwrap_constant()?
            .try_into_string()?;
        part.make_ascii_lowercase();

        let part = part.parse::<date::DatePart>()?;

        match &datatypes[1] {
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_) => {
                Ok(Box::new(DatePartImpl { part }))
            }
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatePartImpl {
    part: date::DatePart,
}

impl PlannedScalarFunction for DatePartImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &DatePart
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        not_implemented!("encode date_part")
    }

    fn return_type(&self) -> DataType {
        DataType::Decimal64(DecimalTypeMeta::new(
            Decimal64Type::MAX_PRECISION,
            Decimal64Type::DEFAULT_SCALE,
        ))
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        // First input ignored (the constant "part" to extract)

        let dec_arr = match inputs[1].as_ref() {
            Array::Date32(arr) => arr.extract_date_part(self.part)?,
            Array::Date64(arr) => arr.extract_date_part(self.part)?,
            Array::Timestamp(arr) => arr.extract_date_part(self.part)?,
            other => return Err(exec_invalid_array_type_err(self, other)),
        };

        Ok(Array::Decimal64(dec_arr))
    }
}
