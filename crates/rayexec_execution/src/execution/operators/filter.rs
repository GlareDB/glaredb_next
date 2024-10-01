use super::simple::{SimpleOperator, StatelessOperation};
use crate::database::DatabaseContext;
use crate::execution::computed_batch::ComputedBatch;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::proto::DatabaseProtoConv;
use rayexec_bullet::{array::Array, batch::Batch, compute::filter::filter};
use rayexec_error::{OptionExt, RayexecError, Result};

pub type PhysicalFilter = SimpleOperator<FilterOperation>;

#[derive(Debug)]
pub struct FilterOperation {
    predicate: PhysicalScalarExpression,
}

impl FilterOperation {
    pub fn new(predicate: PhysicalScalarExpression) -> Self {
        FilterOperation { predicate }
    }
}

impl StatelessOperation for FilterOperation {
    fn execute(&self, batch: ComputedBatch) -> Result<ComputedBatch> {
        // TODO: We should try to skip this. I don't know that the current
        // `select` method is actually correct when multiple selections are
        // performed.
        let batch = batch.try_materialize()?;

        let selection = self.predicate.eval(&batch, None)?;
        let selection = match selection.as_ref() {
            Array::Boolean(arr) => arr.clone().into_selection_bitmap(),
            other => {
                return Err(RayexecError::new(format!(
                    "Expected filter predicate to evaluate to a boolean, got {}",
                    other.datatype()
                )))
            }
        };

        ComputedBatch::try_with_selection(batch, selection)
    }
}

impl Explainable for FilterOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value("predicate", &self.predicate)
    }
}

impl DatabaseProtoConv for PhysicalFilter {
    type ProtoType = rayexec_proto::generated::execution::PhysicalFilter;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            predicate: Some(self.operation.predicate.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            operation: FilterOperation {
                predicate: PhysicalScalarExpression::from_proto_ctx(
                    proto.predicate.required("predicate")?,
                    context,
                )?,
            },
        })
    }
}
