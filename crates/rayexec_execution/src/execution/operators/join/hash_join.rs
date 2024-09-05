use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::TypeSchema;
use rayexec_error::{not_implemented, RayexecError, Result};
use std::task::Context;
use std::{sync::Arc, task::Waker};

use crate::database::DatabaseContext;
use crate::execution::operators::util::hash::{AhashHasher, ArrayHasher};
use crate::execution::operators::{
    ExecutableOperator, ExecutionStates, InputOutputStates, OperatorState, PartitionState,
    PollFinalize, PollPull, PollPush,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::logical::operator::JoinType;

use super::join_hash_table::PartitionJoinHashTable;
use super::outer_join_tracker::{LeftOuterJoinDrainState, LeftOuterJoinTracker};

#[derive(Debug)]
pub struct HashJoinBuildPartitionState {
    /// Hash table this partition will be writing to.
    local_hashtable: PartitionJoinHashTable,

    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,
}

#[derive(Debug)]
pub struct HashJoinProbePartitionState {
    /// Index of this partition.
    partition_idx: usize,

    /// The final output table. If None, the global state should be checked to
    /// see if it's ready to copy into the partition local state.
    global: Option<Arc<PartitionJoinHashTable>>,

    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,

    /// Buffered output batch.
    buffered_output: Option<Batch>,

    /// Waker that's stored from a push if there's already a buffered batch.
    push_waker: Option<Waker>,

    /// Waker that's stored from a pull if there's no batch available.
    pull_waker: Option<Waker>,

    /// If the input for this partiton is complete.
    input_finished: bool,

    /// Track rows visited on the left side for this partition.
    partition_outer_join_tracker: Option<LeftOuterJoinTracker>,

    /// State for tracking rows on the left side that we still need to emit.
    ///
    /// This is currently populated for one partition at the end of probing.
    outer_join_drain_state: Option<LeftOuterJoinDrainState>,
}

impl HashJoinProbePartitionState {
    fn new(partition_idx: usize) -> Self {
        HashJoinProbePartitionState {
            partition_idx,
            global: None,
            hash_buf: Vec::new(),
            buffered_output: None,
            push_waker: None,
            pull_waker: None,
            partition_outer_join_tracker: None,
            input_finished: false,
            outer_join_drain_state: None,
        }
    }
}

#[derive(Debug)]
pub struct HashJoinOperatorState {
    /// Shared state between all partitions.
    inner: Mutex<SharedOutputState>,
}

#[derive(Debug)]
struct SharedOutputState {
    /// The partially built global hash table.
    ///
    /// Input partitions merge their partition-local hash table into this global
    /// table once they complete.
    partial: PartitionJoinHashTable,

    /// Number of build inputs remaining.
    ///
    /// Initially set to number of build partitions.
    build_inputs_remaining: usize,

    /// Number of probe inputs remaining.
    ///
    /// Initially set to number of probe partitions.
    probe_inputs_remaining: usize,

    /// The shared global hash table once it's been fully built.
    ///
    /// This is None if there's still inputs still building.
    shared_global: Option<Arc<PartitionJoinHashTable>>,

    /// Union of all bitmaps across all partitions.
    ///
    /// Referenced with draining unvisited rows in the case of a LEFT join.
    global_outer_join_tracker: Option<LeftOuterJoinTracker>,

    /// Pending wakers for thread that attempted to probe the table prior to it
    /// being built.
    ///
    /// Indexed by probe partition index.
    ///
    /// Woken once the global hash table has been completed (moved into
    /// `shared_global`).
    probe_push_wakers: Vec<Option<Waker>>,
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    /// The type of join we're performing (inner, left, right, semi, etc).
    join_type: JoinType,

    /// Column indices on the left (build) side we're joining on.
    left_on: Vec<usize>,

    /// Column indices on the right (probe) side we're joining on.
    right_on: Vec<usize>,

    /// Types for the batches we'll be receiving from the left side. Used during
    /// RIGHT joins to produce null columns on the left side.
    left_types: Vec<DataType>,

    /// Types for the batches we'll be receiving from the right side. Used
    /// during LEFT joins to produce null columns on the right side.
    right_types: Vec<DataType>,
}

impl PhysicalHashJoin {
    pub const BUILD_SIDE_INPUT_INDEX: usize = 0;
    pub const PROBE_SIDE_INPUT_INDEX: usize = 1;

    pub fn new(
        join_type: JoinType,
        left_on: Vec<usize>,
        right_on: Vec<usize>,
        left_types: TypeSchema,
        right_types: TypeSchema,
    ) -> Self {
        PhysicalHashJoin {
            join_type,
            left_on,
            right_on,
            left_types: left_types.types,
            right_types: right_types.types,
        }
    }
}

impl ExecutableOperator for PhysicalHashJoin {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        // TODO: Determine if this is what we want.
        let build_partitions = partitions[0];
        let probe_partitions = partitions[0];

        let shared_output_state = SharedOutputState {
            partial: PartitionJoinHashTable::new(self.left_types.clone(), self.right_types.clone()),
            build_inputs_remaining: build_partitions,
            probe_inputs_remaining: probe_partitions,
            shared_global: None,
            global_outer_join_tracker: None,
            probe_push_wakers: vec![None; probe_partitions],
        };

        let operator_state = HashJoinOperatorState {
            inner: Mutex::new(shared_output_state),
        };

        let build_states: Vec<_> = (0..build_partitions)
            .map(|_| {
                PartitionState::HashJoinBuild(HashJoinBuildPartitionState {
                    local_hashtable: PartitionJoinHashTable::new(
                        self.left_types.clone(),
                        self.right_types.clone(),
                    ),
                    hash_buf: Vec::new(),
                })
            })
            .collect();

        let probe_states: Vec<_> = (0..probe_partitions)
            .map(|idx| PartitionState::HashJoinProbe(HashJoinProbePartitionState::new(idx)))
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::HashJoin(operator_state)),
            partition_states: InputOutputStates::NaryInputSingleOutput {
                partition_states: vec![build_states, probe_states],
                pull_states: Self::PROBE_SIDE_INPUT_INDEX,
            },
        })
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::HashJoinBuild(state) => {
                let left_columns = self
                    .left_on
                    .iter()
                    .map(|idx| {
                        batch.column(*idx).map(|arr| arr.as_ref()).ok_or_else(|| {
                            RayexecError::new(format!("Missing column at index {idx}"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Compute hashes on input batch
                state.hash_buf.clear();
                state.hash_buf.resize(batch.num_rows(), 0);
                let hashes = AhashHasher::hash_arrays(&left_columns, &mut state.hash_buf)?;

                state.local_hashtable.insert_batch(
                    &batch,
                    hashes,
                    Bitmap::all_true(hashes.len()),
                )?;

                Ok(PollPush::Pushed)
            }
            PartitionState::HashJoinProbe(state) => {
                // If we have pending output, we need to wait for that to get
                // pulled before trying to compute additional batches.
                if state.buffered_output.is_some() {
                    state.push_waker = Some(cx.waker().clone());
                    return Ok(PollPush::Pending(batch));
                }

                let operator_state = match operator_state {
                    OperatorState::HashJoin(state) => state,
                    other => panic!("invalid operator state: {other:?}"),
                };

                // Check if we have the final hash table, if not, look in he
                // global state.
                if state.global.is_none() {
                    let mut shared = operator_state.inner.lock();

                    // If there's still some inputs building, just store our
                    // waker to come back later.
                    if shared.build_inputs_remaining != 0 {
                        shared.probe_push_wakers[state.partition_idx] = Some(cx.waker().clone());
                        return Ok(PollPush::Pending(batch));
                    }

                    // Final partition on the build side should be what sets
                    // this. So if remaining == 0, then it should exist.
                    let shared_global = shared
                        .shared_global
                        .clone()
                        .expect("shared global table should exist, no inputs remaining");

                    // Final hash table built, store in our partition local
                    // state.
                    state.global = Some(shared_global);

                    // Create initial visit bitmaps that will be tracked by this
                    // partition.
                    if self.join_type == JoinType::Left {
                        state.partition_outer_join_tracker =
                            Some(LeftOuterJoinTracker::new_for_batches(
                                state.global.as_ref().unwrap().batches(),
                            ));
                    }
                }

                let hashtable = state.global.as_ref().expect("hash table to exist");

                let right_input_cols = self
                    .right_on
                    .iter()
                    .map(|idx| {
                        batch.column(*idx).map(|arr| arr.as_ref()).ok_or_else(|| {
                            RayexecError::new(format!(
                                "Missing column in probe batch at index {idx}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                state.hash_buf.clear();
                state.hash_buf.resize(batch.num_rows(), 0);
                let hashes = AhashHasher::hash_arrays(&right_input_cols, &mut state.hash_buf)?;

                // TODO: Handle everything else.
                //
                // Left:
                // - Include every unvisited row in left batch, join with right nulls.
                // - Partition local bitmap to track unvisited left batchs.
                // - Flush out unvisited batches on finish.
                //
                // Right:
                // - Include every unvisited row in right batch, join with left nulls.
                // - Nothing else.
                //
                // Outer:
                // - Include every unvisited row in right batch, join with left nulls.
                // - Include every unvisited row in left batch, join with right nulls,
                // - Partition local bitmap to track unvisited left batchs.
                // - Flush out unvisited batches on finish.
                //
                // Left/right semi:
                // - Just include left/right columns.
                //
                // Left/right anti:
                // - Inverse of left/right
                match self.join_type {
                    JoinType::Inner => {
                        let joined =
                            hashtable.probe(&batch, None, hashes, &self.right_on, false)?;
                        state.buffered_output = Some(joined);
                        Ok(PollPush::Pushed)
                    }
                    JoinType::Right => {
                        let joined = hashtable.probe(&batch, None, hashes, &self.right_on, true)?;
                        state.buffered_output = Some(joined);
                        Ok(PollPush::Pushed)
                    }
                    JoinType::Left => {
                        let bitmaps = state.partition_outer_join_tracker.as_mut();
                        let joined =
                            hashtable.probe(&batch, bitmaps, hashes, &self.right_on, false)?;
                        state.buffered_output = Some(joined);
                        Ok(PollPush::Pushed)
                    }
                    other => not_implemented!("join type {other}"),
                }
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let mut shared = match operator_state {
            OperatorState::HashJoin(state) => state.inner.lock(),
            other => panic!("invalid operator state: {other:?}"),
        };

        match partition_state {
            PartitionState::HashJoinBuild(state) => {
                // Merge local table into the global table.
                let local_table = std::mem::replace(
                    &mut state.local_hashtable,
                    PartitionJoinHashTable::new(self.left_types.clone(), self.right_types.clone()),
                );

                shared.partial.merge(local_table)?;

                shared.build_inputs_remaining -= 1;

                // If we're the last remaining, go ahead and move the 'partial'
                // table to 'global', and wake up any pending probers.
                //
                // Probers will then clone the global hash table (behind an Arc)
                // into their local states to avoid needing to synchronize.
                if shared.build_inputs_remaining == 0 {
                    let global_table = std::mem::replace(
                        &mut shared.partial,
                        PartitionJoinHashTable::new(
                            self.left_types.clone(),
                            self.right_types.clone(),
                        ),
                    );
                    shared.shared_global = Some(Arc::new(global_table));

                    for waker in shared.probe_push_wakers.iter_mut() {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                    }
                }

                Ok(PollFinalize::Finalized)
            }
            PartitionState::HashJoinProbe(state) => {
                // Ensure we've finished building the left side before
                // continuing with the finalize.
                //
                // This is important for left joins since we need to flush out
                // unvisited rows which we can only do once we have the complete
                // left side.
                if shared.build_inputs_remaining != 0 {
                    shared.probe_push_wakers[state.partition_idx] = Some(cx.waker().clone());
                    return Ok(PollFinalize::Pending);
                }

                state.input_finished = true;

                // Set partition-local global hash table reference if we don't
                // have it. It's possible for this partition not have this if we
                // pushed no batches for this partition.
                //
                // We want to ensure this is set no matter the join type.
                if state.global.is_none() {
                    state.global = shared.shared_global.clone();
                }

                // Merge local left visit bitmaps into global if we have it.
                match (
                    shared.global_outer_join_tracker.as_mut(),
                    state.partition_outer_join_tracker.as_ref(),
                ) {
                    (Some(global), Some(local)) => global.merge_from(local),
                    (None, Some(local)) => shared.global_outer_join_tracker = Some(local.clone()),
                    (Some(_), None) => {
                        // This can happen if we've pushed nothing for the right. We wouldn't have
                        // initialized the bitmaps before finalizing in that case.
                        //
                        // This is valid.
                    }
                    (None, None) => {
                        // May happen if:
                        //
                        // - Not a left join
                        // - Is a left join but no right partitions have finalized yet.
                    }
                }

                shared.probe_inputs_remaining -= 1;

                // If we're the last probe partition, set up state to drain all
                // unvisited rows from left.
                //
                // TODO: Allow multiple partitions to drain.
                if shared.probe_inputs_remaining == 0 {
                    match shared.global_outer_join_tracker.as_ref() {
                        Some(global) => {
                            state.outer_join_drain_state = Some(LeftOuterJoinDrainState::new(
                                global.clone(),
                                shared.shared_global.as_ref().unwrap().batches().to_vec(),
                                self.left_types.clone(),
                                self.right_types.clone(),
                            ))
                        }
                        None if self.join_type == JoinType::Left => {
                            // Global left bitmaps will be None if we've
                            // received no batches from the right.
                            //
                            // In this case, we'll be draining _all_ batches
                            // from the left.
                            let batches = shared.shared_global.as_ref().unwrap().batches().to_vec();
                            let tracker = LeftOuterJoinTracker::new_for_batches(&batches);

                            state.outer_join_drain_state = Some(LeftOuterJoinDrainState::new(
                                tracker,
                                shared.shared_global.as_ref().unwrap().batches().to_vec(),
                                self.left_types.clone(),
                                self.right_types.clone(),
                            ));
                        }
                        None => {
                            // Nothing to do.
                        }
                    }
                }

                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }
                Ok(PollFinalize::Finalized)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::HashJoinProbe(state) => state,
            PartitionState::HashJoinBuild(_) => {
                // We should only be pulling with the "probe" state. The "build"
                // state acts as a sink into the operator.
                panic!("should not pull with a build state")
            }
            other => panic!("invalid partition state: {other:?}"),
        };

        match state.buffered_output.take() {
            Some(batch) => {
                // Partition has space available, go ahead an wake a pending
                // pusher.
                if let Some(waker) = state.push_waker.take() {
                    waker.wake();
                }

                Ok(PollPull::Batch(batch))
            }
            None => {
                if state.input_finished {
                    // Check if we're still draining unvisited left rows.
                    if let Some(drain_state) = state.outer_join_drain_state.as_mut() {
                        match drain_state.drain_next()? {
                            Some(batch) => return Ok(PollPull::Batch(batch)),
                            None => return Ok(PollPull::Exhausted),
                        }
                    }

                    // We're done.
                    return Ok(PollPull::Exhausted);
                }

                // No batch available, come back later.
                state.pull_waker = Some(cx.waker().clone());

                // Wake up a pusher since there's space available.
                if let Some(waker) = state.push_waker.take() {
                    waker.wake();
                }

                Ok(PollPull::Pending)
            }
        }
    }
}

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashJoin")
    }
}
