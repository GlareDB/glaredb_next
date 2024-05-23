use crate::{
    execution::operators::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush},
    expr::PhysicalSortExpression,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

#[derive(Debug)]
pub struct MergeSortedPushPartitionState {}

#[derive(Debug)]
pub struct MergeSortedPullPartitionState {}

#[derive(Debug)]
pub struct MergeSortedOperatorState {}

/// Merge sorted partitions into a single output partition.
#[derive(Debug)]
pub struct PhysicalMergeSortedInputs {
    exprs: Vec<PhysicalSortExpression>,
}

impl PhysicalMergeSortedInputs {}

impl PhysicalOperator for PhysicalMergeSortedInputs {
    fn poll_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}
