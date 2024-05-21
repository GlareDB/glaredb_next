use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::{Context, Waker};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct LimitPartitionState {
    /// Remaining offset before we can actually start sending batches.
    remaining_offset: usize,

    /// Remaining limit before we stop sending batches.
    remaining_limit: usize,

    /// A buffered batch.
    buffer: Option<Batch>,

    /// Waker on pull side if no batch is ready.
    pull_waker: Option<Waker>,

    /// Waker on push side if this partition is already buffering an output
    /// batch.
    push_waker: Option<Waker>,

    /// If inputs are finished.
    finished: bool,
}

/// Operator for LIMIT and OFFSET clauses.
///
/// The provided `limit` and `offset` values work on a per-partition basis. A
/// global limit/offset should be done by using a single partition.
#[derive(Debug)]
pub struct PhysicalLimit {
    /// Number of rows to limit to.
    limit: usize,

    /// Offset to start limiting from.
    offset: Option<usize>,
}

impl PhysicalLimit {
    pub fn new(limit: usize, offset: Option<usize>) -> Self {
        PhysicalLimit { limit, offset }
    }

    /// Create states for this operator.
    ///
    /// Limit has no global states, only partition-local states.
    pub fn create_states(&self, partitions: usize) -> Vec<LimitPartitionState> {
        (0..partitions)
            .map(|_| LimitPartitionState {
                remaining_limit: self.limit,
                remaining_offset: self.offset.unwrap_or(0),
                buffer: None,
                pull_waker: None,
                push_waker: None,
                finished: false,
            })
            .collect()
    }
}

impl PhysicalOperator for PhysicalLimit {
    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}
