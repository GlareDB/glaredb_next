use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::Context;
use std::{sync::Arc, task::Waker};

use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};

use super::grouping_set::GroupingSets;

#[derive(Debug)]
pub struct HashAggregateOperatorState {}

#[derive(Debug)]
pub struct HashAggregatePartitionState {}

#[derive(Debug)]
pub struct PhysicalHashAggregate {
    /// Grouping sets we're grouping by.
    grouping_sets: GroupingSets,
}

impl PhysicalOperator for PhysicalHashAggregate {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<()> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}
