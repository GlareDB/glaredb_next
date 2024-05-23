use crate::{
    execution::operators::{
        sort::util::k_way_merge::KWayMergeExhaustBehavior, OperatorState, PartitionState,
        PhysicalOperator, PollPull, PollPush,
    },
    expr::PhysicalSortExpression,
};
use parking_lot::Mutex;
use rayexec_bullet::{batch::Batch, row::encoding::ComparableRows};
use rayexec_error::Result;
use std::task::{Context, Waker};

use super::util::{
    k_way_merge::{RestartableKWayState, SortedBatch},
    sort_keys::SortKeysExractor,
};

/// Partition state on the push side.
#[derive(Debug)]
pub struct MergeSortedPushPartitionState {
    /// Index of this partition. Used to emplace buffered batches into the
    /// global state.
    partition_idx: usize,
}

/// Partition state on the pull side.
#[derive(Debug)]
pub struct MergeSortedPullPartitionState {
    /// Inputs that will be merged.
    inputs: Vec<PullInput>,

    /// State stored between pulls to pick up where we left on from the previous
    /// merge.
    merge_state: RestartableKWayState,
    // TODO: `limit` for use with top k
}

#[derive(Debug)]
struct PullInput {
    /// Input batch that will be part of the merge.
    ///
    /// If None and input not finished, the global state needs to be checked.
    batch: Option<SortedBatch>,

    /// If the input partition is finished.
    finished: bool,
}

#[derive(Debug)]
pub struct MergeSortedOperatorState {
    shared: Mutex<SharedGlobalState>,
}

#[derive(Debug)]
struct SharedGlobalState {
    /// Batches from the input partitions.
    ///
    /// Indexed by input partition_idx.
    batches: Vec<Option<SortedBatch>>,

    /// If input partitions are finished.
    ///
    /// Indexed by input partition_idx.
    finished: Vec<bool>,

    /// Wakers on the push side.
    ///
    /// If the input partition already has batch in the global shared state,
    /// it'll be marked pending.
    ///
    /// Indexed by input partition_idx.
    push_wakers: Vec<Option<Waker>>,

    /// Waker from the pull side if it doesn't have at least one batch from each
    /// input.
    ///
    /// Paired with the index of the input partition that the pull side is
    /// waiting for.
    ///
    /// Waken only when the specified input partition is able to place a batch
    /// into the global state (or finishes).
    pull_waker: (usize, Option<Waker>),
}

/// Merge sorted partitions into a single output partition.
#[derive(Debug)]
pub struct PhysicalMergeSortedInputs {
    exprs: Vec<PhysicalSortExpression>,
    extractor: SortKeysExractor,
}

impl PhysicalMergeSortedInputs {}

impl PhysicalOperator for PhysicalMergeSortedInputs {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::MergeSortedPush(state) => state,
            PartitionState::MergeSortedPull(_) => {
                panic!("uses pull state when push state expected")
            }
            other => panic!("invalid partition state: {other:?}"),
        };

        let mut shared = match operator_state {
            OperatorState::MergeSorted(state) => state.shared.lock(),
            other => panic!("invalid operator state: {other:?}"),
        };

        if shared.batches[state.partition_idx].is_some() {
            // Can't push, global state already has a batch for this partition.
            shared.push_wakers[state.partition_idx] = Some(cx.waker().clone());
            return Ok(PollPush::Pending(batch));
        }

        let sorted = SortedBatch {
            batch,
            keys: self.extractor.sort_keys(&batch)?,
        };
        shared.batches[state.partition_idx] = Some(sorted);

        // Wake up the pull side if its waiting on this partition.
        if shared.pull_waker.0 == state.partition_idx {
            if let Some(waker) = shared.pull_waker.1.take() {
                waker.wake();
            }
        }

        Ok(PollPush::Pushed)
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<()> {
        let state = match partition_state {
            PartitionState::MergeSortedPush(state) => state,
            PartitionState::MergeSortedPull(_) => {
                panic!("uses pull state when push state expected")
            }
            other => panic!("invalid partition state: {other:?}"),
        };

        let mut shared = match operator_state {
            OperatorState::MergeSorted(state) => state.shared.lock(),
            other => panic!("invalid operator state: {other:?}"),
        };

        shared.finished[state.partition_idx] = true;

        // Wake up the pull side if its waiting on this partition.
        if shared.pull_waker.0 == state.partition_idx {
            if let Some(waker) = shared.pull_waker.1.take() {
                waker.wake();
            }
        }

        Ok(())
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::MergeSortedPull(state) => state,
            PartitionState::MergeSortedPush(_) => {
                panic!("uses push state when pull state expected")
            }
            other => panic!("invalid partition state: {other:?}"),
        };

        // Check that we have batches from all inputs. If we do, we can avoid
        // taking a lock on the global state.
        if state
            .inputs
            .iter()
            .all(|input| input.batch.is_some() || input.finished)
        {
            // Otherwise we need to get batches from the global state and move
            // them this the local state.
            let mut shared = match operator_state {
                OperatorState::MergeSorted(state) => state.shared.lock(),
                other => panic!("invalid operator state: {other:?}"),
            };

            for (input_idx, input) in state.inputs.iter().enumerate() {
                if input.batch.is_some() || input.finished {
                    continue;
                }

                match shared.batches[input_idx].take() {
                    Some(batch) => state.inputs[input_idx].batch = Some(batch),
                    None => {
                        if shared.finished[input_idx] {
                            state.inputs[input_idx].finished = true;
                            // Continue, we have other batches to check.
                        } else {
                            // Batch not yet available for this input partition.
                            shared.pull_waker = (input_idx, Some(cx.waker().clone()));
                            return Ok(PollPull::Pending);
                        }
                    }
                }
            }
        }

        // Now... we do some merging!

        let batches: Vec<_> = state
            .inputs
            .iter()
            .map(|input| {
                if input.finished {
                    None
                } else {
                    // This is done above.
                    Some(&input.batch.expect("batch to exist"))
                }
            })
            .collect();

        let mut merger = state.merge_state.create_merger(batches)?;
        let indices = match merger.interleave_indices(KWayMergeExhaustBehavior::Break, 1024) {
            Some(indices) => indices,
            None => return Ok(PollPull::Exhausted),
        };

        // let mut merged_columns = Vec::with_capacity(self.columns.len());
        // for column in &self.columns {
        //     let merged = compute::interleave::interleave(&column.columns, indices)?;
        //     merged_columns.push(merged);
        // }

        // let batch = Batch::try_new(merged_columns)?;

        unimplemented!()
    }
}
