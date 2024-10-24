use std::collections::VecDeque;
use std::sync::Arc;
use std::task::{Context, Waker};

use rayexec_bullet::batch::Batch;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::Result;

use super::{
    ExecutableOperator,
    ExecutionStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

// TODO: Provide when creating states.
//
// TODO: It might also make sense to have lower/upper bounds to reduce
// reallocating. E.g. if we get an input batch of 4095, do we really care that
// it's not 4096?
const TARGET_BATCH_SIZE: usize = 4096;

#[derive(Debug)]
pub struct BatchResizerPartitionState {
    /// Input batches that we're attempting to concat together.
    input_batches: VecDeque<Batch>,
    /// Pending output batch.
    output_batch: Option<Batch>,
    /// Push waker that's set if `output_batch` is Some.
    push_waker: Option<Waker>,
    /// Pull waker that's set if `output_batch` is None.
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
struct InputBatches {
    /// Pending batches we need to concat.
    batches: Vec<Batch>,
    /// Total row count of all pending batches.
    pending_rows: usize,
}

impl InputBatches {
    /// Try to push a batch.
    ///
    /// If the current number of pending batches reaches the target length, then
    /// those batches will be concatenated into a single batch and returned.
    fn try_push(&mut self, batch: Batch) -> Result<Option<Batch>> {
        if self.pending_rows + batch.num_rows() == TARGET_BATCH_SIZE {
            self.batches.push(batch);

            // Concat, return, set pending len = 0

            unimplemented!()
        }

        if self.pending_rows + batch.num_rows() > TARGET_BATCH_SIZE {
            let diff = (self.pending_rows + batch.num_rows()) - TARGET_BATCH_SIZE;

            // Generate selection vectors that logically slice this batch.
            //
            // Batch 'a' will be included in the current set of batches that
            // will concatenated, batch 'b' will initialize the next set.
            let sel_a = SelectionVector::with_range(0..diff);
            let sel_b = SelectionVector::with_range(diff..batch.num_rows());

            let batch_a = batch.select(Arc::new(sel_a));
            let batch_b = batch.select(Arc::new(sel_b));

            self.batches.push(batch_a);

            {
                // Concat, clear vec, push batch b, set pending len = len(b)
                unimplemented!()
            }
        }

        // Otherwise just add to pending batches.
        self.pending_rows += batch.num_rows();
        self.batches.push(batch);

        Ok(None)
    }
}

/// Operator that collects some of input batches to produce batches of a target
/// size.
#[derive(Debug)]
pub struct PhysicalBatchResizer {}

impl ExecutableOperator for PhysicalBatchResizer {
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

impl Explainable for PhysicalBatchResizer {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("BatchResizer")
    }
}
