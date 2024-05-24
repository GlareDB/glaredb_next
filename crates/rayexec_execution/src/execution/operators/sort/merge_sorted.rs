use crate::{
    execution::operators::{
        sort::util::merger::IterState, OperatorState, PartitionState, PhysicalOperator, PollPull,
        PollPush,
    },
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
    /// Partition-local buffers for input batches.
    ///
    /// To avoid taking the global lock too frequently, we try to copy in as
    /// many batches as possible into the local state every time we look at the
    /// global state.
    input_buffers: InputBuffers,

    merge_state: PullMergeState,
}

#[derive(Debug)]
struct InputBuffers {
    /// Buffered batches that we retrieve from the global state to avoid needing
    /// to acquire the global lock.
    ///
    /// Indexed by input partition idx.
    buffered: Vec<Option<PhysicallySortedBatch>>,

    /// If each input is finished.
    ///
    /// Indexed by input partition idx.
    finished: Vec<bool>,
}

#[derive(Debug)]
enum PullMergeState {
    /// Currently initialing the state.
    ///
    /// We need at least one batch (or finished==true) from each input input
    /// partition before being able to produce output batches.
    Initializing,

    /// Able to start producing output.
    Producing {
        /// Partitions index for an input that's required. Merging will not
        /// continue until we have a batch from this input.
        input_required: Option<usize>,

        /// Merger for input batches.
        merger: KWayMerger<SortedKeysIter>,
    },
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
        let pull_states = vec![MergeSortedPullPartitionState {
            input_buffers: InputBuffers {
                buffered: (0..input_partitions).map(|_| None).collect(),
                finished: (0..input_partitions).map(|_| false).collect(),
            },
            merge_state: PullMergeState::Initializing,
        }];

        (operator_state, push_states, pull_states)
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

        // Finish up initialization if needed.
        if let PullMergeState::Initializing = &state.merge_state {
            match Self::try_finish_initialize(cx, &mut state.input_buffers, operator_state)? {
                Some(merger) => {
                    // Flip state and continue.
                    state.merge_state = PullMergeState::Producing {
                        input_required: None,
                        merger,
                    }
                }
                None => {
                    // Not finished initializing, still waiting on some input.
                    //
                    // `try_finish_initialize` registers a waker for us.
                    return Ok(PollPull::Pending);
                }
            }
        }

        match &mut state.merge_state {
            PullMergeState::Producing {
                input_required,
                merger,
            } => {
                if let Some(input_idx) = input_required {
                    let input_pushed = Self::try_push_input_batch_to_merger(
                        cx,
                        merger,
                        &mut state.input_buffers,
                        operator_state,
                        *input_idx,
                    )?;
                    if !input_pushed {
                        // `try_push_input_batch_to_merger` registers a waker for us.
                        return Ok(PollPull::Pending);
                    }

                    // Input no longer required, we've either pushed the batch
                    // to the merger, or let the merger know that that input's
                    // finished.
                    *input_required = None;
                }

                // Now try to merge.
                //
                // We loop to try to make as much progress with the merger using
                // our local buffered batches as much as possible.
                loop {
                    // TODO: Configurable batch size.
                    match merger.try_merge(1024)? {
                        MergeResult::Batch(batch) => return Ok(PollPull::Batch(batch)),
                        MergeResult::NeedsInput(input_idx) => {
                            match state.input_buffers.buffered[input_idx].take() {
                                Some(batch) => {
                                    let (batch, iter) = batch.into_batch_and_iter();
                                    merger.push_batch_for_input(input_idx, batch, iter);
                                    continue; // Keep trying to merge.
                                }
                                None => {
                                    let pushed = Self::try_push_input_batch_to_merger(
                                        cx,
                                        merger,
                                        &mut state.input_buffers,
                                        operator_state,
                                        input_idx,
                                    )?;

                                    if pushed {
                                        // Keep trying to merge
                                        continue;
                                    } else {
                                        // Batch not availab yet. Waker
                                        // registered by
                                        // `try_push_input_batch_to_merger`.
                                        return Ok(PollPull::Pending);
                                    }
                                }
                            }
                        }
                        MergeResult::Exhausted => return Ok(PollPull::Exhausted),
                    }
                }
            }
            PullMergeState::Initializing => unreachable!("should should be 'producing' by now"),
        }
    }
}

impl PhysicalMergeSortedInputs {
    /// Try to finish the pull-side initialization step.
    ///
    /// This will try to get a batch from each input partition so we can
    /// initialize the merger. If we are able to initialize the merger, the
    /// partition's merge state is flipped to Producing.
    ///
    /// Returns the initialized merger on success.
    ///
    /// If this returns None, our waker will be registered in the global state
    /// for the input partition we're waiting on.
    fn try_finish_initialize(
        cx: &mut Context,
        input_buffers: &mut InputBuffers,
        operator_state: &MergeSortedOperatorState,
    ) -> Result<Option<KWayMerger<SortedKeysIter>>> {
        let mut shared = operator_state.shared.lock();
        for (idx, local_buf) in input_buffers.buffered.iter_mut().enumerate() {
            if local_buf.is_none() {
                *local_buf = shared.batches[idx].take();
            }
            input_buffers.finished[idx] = shared.finished[idx];
        }

        // Find the partition index that we still need input for.
        let need_partition = input_buffers
            .buffered
            .iter()
            .zip(input_buffers.finished.iter())
            .position(|(batch, finished)| batch.is_none() && !finished);

        match need_partition {
            Some(partition_idx) => {
                // Need to wait for input from this partition.
                shared.pull_waker = (partition_idx, Some(cx.waker().clone()));
                Ok(None)
            }
            None => {
                // Otherwise we can begin merging.
                std::mem::drop(shared); // No need to keep the lock.

                let mut inputs = Vec::with_capacity(input_buffers.buffered.len());
                for (batch, finished) in input_buffers
                    .buffered
                    .iter_mut()
                    .zip(input_buffers.finished.iter())
                {
                    match batch.take() {
                        Some(batch) => {
                            let (batch, iter) = batch.into_batch_and_iter();
                            inputs.push((Some(batch), IterState::Iterator(iter)));
                        }
                        None => {
                            assert!(
                                finished,
                                "partition input must be finished if no batches produced"
                            );
                            inputs.push((None, IterState::Finished));
                        }
                    }
                }
                assert_eq!(inputs.len(), input_buffers.buffered.len());

                let merger = KWayMerger::try_new(inputs)?;
                // We're good, state's been flipped and we can continue with the
                // pull.
                Ok(Some(merger))
            }
        }
    }

    /// Try to push an batch for input indicated by `input_idx` to the merger.
    ///
    /// This will first check to see if we have a batch ready in the
    /// partition-local state. If not, we'll then check the global state.
    ///
    /// If we are able to get a batch for the input, or we see that the input is
    /// finished, the merger will be updated with that info. Otherwise, we
    /// register our waker in the global state, and we need to wait.
    ///
    /// Returns true on successfully getting a batch (or seeing that
    /// finished==true). Returns false otherwise.
    fn try_push_input_batch_to_merger(
        cx: &mut Context,
        merger: &mut KWayMerger<SortedKeysIter>,
        input_buffers: &mut InputBuffers,
        operator_state: &MergeSortedOperatorState,
        input_idx: usize,
    ) -> Result<bool> {
        // We need to make sure we have a batch from this input. Try from the
        // partition state first, then look in the global state.
        match input_buffers.buffered[input_idx].take() {
            Some(batch) => {
                // We're good, go ahead and given this batch to the
                // merger and keep going.
                let (batch, iter) = batch.into_batch_and_iter();
                merger.push_batch_for_input(input_idx, batch, iter);

                Ok(true)
            }
            None => {
                // Check if this input is finished, and let the
                // merger know if so.
                if input_buffers.finished[input_idx] {
                    merger.input_finished(input_idx);
                    Ok(true)
                } else {
                    // Otherwise need to get from global input_buffers.
                    let mut shared = operator_state.shared.lock();

                    // Copy in as many batches as we can from the
                    // global input_buffers.
                    for (idx, local_buf) in input_buffers.buffered.iter_mut().enumerate() {
                        if local_buf.is_none() {
                            *local_buf = shared.batches[idx].take();
                        }
                        input_buffers.finished[idx] = shared.finished[idx];
                    }

                    // Check local input_buffers again.
                    match (
                        input_buffers.buffered[input_idx].take(),
                        input_buffers.finished[input_idx],
                    ) {
                        (Some(batch), false) => {
                            // We have a batch read to go, give it
                            // to the merge and continue.
                            let (batch, iter) = batch.into_batch_and_iter();
                            merger.push_batch_for_input(input_idx, batch, iter);
                            Ok(true)
                        }
                        (None, true) => {
                            // Input is finished, let the merger
                            // know and continue.
                            merger.input_finished(input_idx);
                            Ok(true)
                        }
                        (None, false) => {
                            // Need to wait for a batch, register
                            // our waker, and return Pending.
                            shared.pull_waker = (input_idx, Some(cx.waker().clone()));
                            Ok(false)
                        }
                        (Some(_), true) => panic!("invalid state"),
                    }
                }
            }
        }
    }
}
