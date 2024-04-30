use crate::expr::PhysicalScalarExpression;
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::DataBatch;

use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use super::PhysicalOperator;

#[derive(Debug)]
pub struct PhysicalProjection {
    exprs: Vec<PhysicalScalarExpression>,
}

impl PhysicalProjection {
    pub fn try_new(exprs: Vec<PhysicalScalarExpression>) -> Result<Self> {
        Ok(PhysicalProjection { exprs })
    }
}

impl PhysicalOperator for PhysicalProjection {
    fn execute(&self, _task_cx: &TaskContext, input: Batch) -> Result<Batch> {
        let arrs = self
            .exprs
            .iter()
            .map(|expr| expr.eval(&input))
            .collect::<Result<Vec<_>>>()?;

        let batch = Batch::try_new(arrs)?;

        Ok(batch)
    }
}

impl Explainable for PhysicalProjection {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Projection")
    }
}
