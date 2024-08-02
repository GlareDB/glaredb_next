use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::Context;

use crate::database::DatabaseContext;
use crate::execution::operators::{
    ExecutableOperator, ExecutionStates, OperatorState, PartitionState, PollFinalize, PollPull,
    PollPush,
};
use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct TopKPartitionState {}

#[derive(Debug)]
pub struct TopKOperatorState {}

#[derive(Debug)]
pub struct PhysicalTopK {}

impl ExecutableOperator for PhysicalTopK {
    fn operator_name(&self) -> &'static str {
        "top_k"
    }

    fn create_states(
        &self,
        _context: &DatabaseContext,
        _partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        unimplemented!()
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}

impl Explainable for PhysicalTopK {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TopK")
    }
}
