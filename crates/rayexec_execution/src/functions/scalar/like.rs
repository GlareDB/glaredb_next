use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UnaryExecutor};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::util_types;
use serde::{Deserialize, Serialize};

use super::comparison::EqImpl;
use super::{PlannedScalarFunction, ScalarFunction};
use crate::expr::Expression;
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Like;

impl FunctionInfo for Like {
    fn name(&self) -> &'static str {
        "like"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            // like(input, pattern)
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
        ]
    }
}

impl ScalarFunction for Like {
    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        unimplemented!()
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

        match (&datatypes[0], &datatypes[1]) {
            (DataType::Utf8, DataType::Utf8) => (),
            (DataType::LargeUtf8, DataType::LargeUtf8) => (),
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        }

        let pattern = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(bind_context, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Some(pattern)
        } else {
            None
        };

        Ok(Box::new(LikeImpl { constant: pattern }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeImpl {
    pub constant: Option<String>,
}

impl PlannedScalarFunction for LikeImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Like
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        Err(RayexecError::new("what"))
    }
}
