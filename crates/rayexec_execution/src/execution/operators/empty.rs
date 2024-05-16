use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::Context;

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug, Default)]
pub struct EmptyPartitionState {
    finished: bool,
}

#[derive(Debug)]
pub struct PhysicalEmpty;

impl PhysicalOperator for PhysicalEmpty {
    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to physical empty"))
    }

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        Err(RayexecError::new("Cannot push to physical empty"))
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Empty(state) => {
                if state.finished {
                    Ok(PollPull::Exhausted)
                } else {
                    state.finished = true;
                    Ok(PollPull::Batch(Batch::empty_with_num_rows(1)))
                }
            }
            other => panic!("inner join state is not building: {other:?}"),
        }
    }
}
