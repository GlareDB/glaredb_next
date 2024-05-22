use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};

use super::sort_data::PartitionSortData;

#[derive(Debug)]
pub struct OrderByPartitionState {
    sort_data: PartitionSortData,
}

#[derive(Debug)]
pub struct OrderByOperatorState {}

#[derive(Debug)]
pub struct PhysicalOrderBy {}

impl PhysicalOrderBy {
    // TODO: Configurable output partitions?
    pub fn create_states(
        &self,
        input_partitions: usize,
    ) -> (OrderByOperatorState, Vec<OrderByPartitionState>) {
        unimplemented!()
    }
}

impl PhysicalOperator for PhysicalOrderBy {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::OrderBy(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.sort_data.push_batch(batch)?;

        // TODO: When merge?

        Ok(PollPush::NeedsMore)
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        let state = match partition_state {
            PartitionState::OrderBy(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        // TODO: Merge here?

        Ok(())
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::OrderBy(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        // TODO: Or maybe here?

        unimplemented!()
    }
}
