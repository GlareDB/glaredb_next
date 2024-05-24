use crate::{
    execution::operators::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush},
    expr::PhysicalSortExpression,
};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

use super::util::{
    merger::{KWayMerger, MergeResult},
    sort_keys::SortKeysExtractor,
    sorted_batch::{PhysicallySortedBatch, SortedKeysIter},
};

/// Partition state on the push side.
#[derive(Debug)]
pub struct MergeSortedPushPartitionState {
    /// Index of this partition. Used to emplace buffered batches into the
    /// global state.
    partition_idx: usize,

    /// Extract the sort keys from a batch.
    extractor: SortKeysExtractor,
}

/// Partition state on the pull side.
#[derive(Debug)]
pub struct MergeSortedPullPartitionState {
    /// Buffered batches that we retrieve from the global state to avoid needing
    /// to acquire the global lock.
    ///
    /// Indexed by input partition idx.
    buffered: Vec<Option<PhysicallySortedBatch>>,

    /// If each input is finished.
    ///
    /// Indexed by input partition idx.
    finished: Vec<bool>,

    /// Next input we need to try to fetch. If this is Some, we _must_ get a
    /// batch for this input before attempting to merge again.
    fetch_input: Option<usize>,

    /// K-way merger for inputs.
    merger: KWayMerger<SortedKeysIter>,
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
    batches: Vec<Option<PhysicallySortedBatch>>,

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

impl SharedGlobalState {
    fn new(num_partitions: usize) -> Self {
        let batches: Vec<_> = (0..num_partitions).map(|_| None).collect();
        let finished: Vec<_> = (0..num_partitions).map(|_| false).collect();
        let push_wakers: Vec<_> = (0..num_partitions).map(|_| None).collect();

        SharedGlobalState {
            batches,
            finished,
            push_wakers,
            pull_waker: (0, None),
        }
    }
}

/// Merge sorted partitions into a single output partition.
#[derive(Debug)]
pub struct PhysicalMergeSortedInputs {
    exprs: Vec<PhysicalSortExpression>,
}

impl PhysicalMergeSortedInputs {
    pub fn new(exprs: Vec<PhysicalSortExpression>) -> Self {
        PhysicalMergeSortedInputs { exprs }
    }

    pub fn create_states(
        &self,
        input_partitions: usize,
    ) -> (
        MergeSortedOperatorState,
        Vec<MergeSortedPushPartitionState>,
        Vec<MergeSortedPullPartitionState>,
    ) {
        let operator_state = MergeSortedOperatorState {
            shared: Mutex::new(SharedGlobalState::new(input_partitions)),
        };

        let extractor = SortKeysExtractor::new(&self.exprs);

        let push_states: Vec<_> = (0..input_partitions)
            .map(|idx| MergeSortedPushPartitionState {
                partition_idx: idx,
                extractor: extractor.clone(),
            })
            .collect();

        // Note vec with a single element representing a single output
        // partition.
        //
        // I'm not sure if we care to support multiple output partitions, but
        // extending this a little could provide an interesting repartitioning
        // scheme where we repartition based on the sort key.
        // let pull_states = vec![MergeSortedPullPartitionState {
        //     buffered: (0..input_partitions).map(|_| None).collect(),
        //     finished: (0..input_partitions).map(|_| false).collect(),
        //     fetch_input: None,
        //     merger: KWayMerger::new(input_partitions),
        // }];

        // (operator_state, push_states, pull_states)
        unimplemented!()
    }
}

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

        let keys = state.extractor.sort_keys(&batch)?;
        let sorted = PhysicallySortedBatch { batch, keys };
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

        let operator_state = match operator_state {
            OperatorState::MergeSorted(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        pull_merge(cx, state, operator_state)
    }
}

/// Try to pull a batch by merging input batches.
fn pull_merge(
    cx: &mut Context,
    state: &mut MergeSortedPullPartitionState,
    operator_state: &MergeSortedOperatorState,
) -> Result<PollPull> {
    // If we're waiting on a particular input, make sure we can fetch that
    // before attempting to merge.
    if let Some(idx) = state.fetch_input {
        let mut shared = operator_state.shared.lock();
        match shared.batches[idx].take() {
            Some(batch) => {
                state.buffered[idx] = Some(batch);
                state.fetch_input = None; // We can continue.
            }
            None => {
                state.finished[idx] = shared.finished[idx];
                if state.finished[idx] {
                    state.merger.input_finished(idx);
                    // We can continue...
                } else {
                    // Otherwise we have to wait.
                    shared.pull_waker = (idx, Some(cx.waker().clone()));
                    return Ok(PollPull::Pending);
                }
            }
        }
    }

    // TODO: Configure batch size.
    match state.merger.try_merge(1024)? {
        MergeResult::Batch(batch) => Ok(PollPull::Batch(batch)),
        MergeResult::NeedsInput(idx) => {
            // println!("needs input: {idx}");
            // // If we have a batch in out local state, go ahead and used that.
            // //
            // // Once added to the merger, try merging again.
            // if let Some(batch) = state.buffered[idx].take() {
            //     let (batch, iter) = batch.into_batch_and_iter(idx);
            //     state.merger.push_batch_for_input(idx, batch, iter);
            //     return pull_merge(cx, state, operator_state);
            // }

            // // Otherwise we need to get a batch from the global state.
            // {
            //     let mut shared = operator_state.shared.lock();

            //     match shared.batches[idx].take() {
            //         Some(batch) => {
            //             // Push the batch to the merger.
            //             let (batch, iter) = batch.into_batch_and_iter(idx);
            //             state.merger.push_batch_for_input(idx, batch, iter);
            //             // And continue..
            //         }
            //         None => {
            //             // Batch not ready yet, we need to check back later.
            //             state.fetch_input = Some(idx); // Prevent merging prior to getting this input.
            //             shared.pull_waker = (idx, Some(cx.waker().clone()));
            //             return Ok(PollPull::Pending);
            //         }
            //     }

            //     // As an optimization, before releasing the lock, we try to take
            //     // as much as possible from the global state and move it to
            //     // partition state.
            //     for (partition_idx, buffered) in state.buffered.iter_mut().enumerate() {
            //         if buffered.is_none() {
            //             *buffered = shared.batches[partition_idx].take();
            //         }
            //         state.finished[partition_idx] = shared.finished[partition_idx];
            //     }
            // }

            // We're good to try to merge again. We have the
            pull_merge(cx, state, operator_state)
        }
        MergeResult::Exhausted => Ok(PollPull::Exhausted),
    }
}
