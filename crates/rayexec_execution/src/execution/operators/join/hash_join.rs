use parking_lot::Mutex;
use rayexec_bullet::array::{Array, BooleanArray};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::compute::filter::filter;
use rayexec_error::{RayexecError, Result};
use std::task::Context;
use std::{sync::Arc, task::Waker};

use crate::execution::operators::util::hash::{hash_arrays, partition_for_hash};
use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};
use crate::planner::operator::JoinType;

use super::join_hash_table::PartitionJoinHashTable;

#[derive(Debug)]
pub struct HashJoinBuildPartitionState {
    /// Hash tables we'll be writing to, one per output partition.
    output_hashtables: Vec<PartitionJoinHashTable>,

    /// Reusable hashes buffer.
    hash_buf: Vec<u64>,

    /// Resusable partitions buffer.
    partitions_idx_buf: Vec<usize>,
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

    /// Waker that's stored if there's already a buffered batch.
    push_waker: Option<Waker>,
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
    remaining: usize,

    /// The shared global hash table once it's been fully built.
    ///
    /// This is None if there's still inputs still building.
    shared_global: Option<Arc<PartitionJoinHashTable>>,

    /// Pending wakers for threads that attempted to pull from the table prior
    /// to it being built.
    ///
    /// Indexed by output partition index.
    pull_wakers: Vec<Option<Waker>>,

    /// Pending wakers for thread that attempted to probe the table prior to it
    /// being built.
    ///
    /// Indexed by probe partition index.
    probe_push_waker: Vec<Option<Waker>>,
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    /// The type of join we're performing (inner, left, right, semi, etc).
    join_type: JoinType,

    /// Column indices on the left (build) side we're joining on.
    left_on: Vec<usize>,

    /// Column indices on the right (probe) side we're joining on.
    right_on: Vec<usize>,
}

impl PhysicalHashJoin {
    pub fn new(join_type: JoinType, left_on: Vec<usize>, right_on: Vec<usize>) -> Self {
        PhysicalHashJoin {
            join_type,
            left_on,
            right_on,
        }
    }

    /// Create states for this operator.
    ///
    /// The number of partition inputs on the build side may be different than
    /// the number of partitions on the probe side.
    ///
    /// Output partitions equals the number of probe side input partitions.
    pub fn create_states(
        &self,
        build_partitions: usize,
        probe_partitions: usize,
    ) -> (
        HashJoinOperatorState,
        Vec<HashJoinBuildPartitionState>,
        Vec<HashJoinProbePartitionState>,
    ) {
        unimplemented!()
    }
}

impl PhysicalOperator for PhysicalHashJoin {
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

                // Compute hashes on input batch, compute output batch for each
                // row based on the hash.
                let hashes = hash_arrays(&left_columns, &mut state.hash_buf)?;
                let partition_indices = &mut state.partitions_idx_buf;
                partition_indices.clear();
                for hash in hashes.iter() {
                    partition_indices
                        .push(partition_for_hash(*hash, state.output_hashtables.len()));
                }

                // Split batch up into multiple smaller batches and insert into
                // the requisite output hashtable.
                for (partition_idx, hashtable) in state.output_hashtables.iter_mut().enumerate() {
                    let selection = Bitmap::from_iter(
                        partition_indices.iter().map(|idx| *idx == partition_idx),
                    );
                    hashtable.insert_batch(&batch, hashes, selection)?;
                }

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
                    if shared.remaining != 0 {
                        shared.probe_push_waker[state.partition_idx] = Some(cx.waker().clone());
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
                let hashes = hash_arrays(&right_input_cols, &mut state.hash_buf)?;

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
                        let joined = hashtable.probe(&batch, hashes, &self.right_on)?;
                        state.buffered_output = Some(joined);
                        Ok(PollPush::Pushed)
                    }
                    JoinType::Left => {
                        unimplemented!()
                    }
                    JoinType::Right => {
                        unimplemented!()
                    }
                    JoinType::Full => {
                        unimplemented!()
                    }
                }
            }
            other => panic!("invalid partition state: {other:?}"),
        }
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
