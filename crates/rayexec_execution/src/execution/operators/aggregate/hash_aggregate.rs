use rayexec_bullet::array::{Array, NullArray};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_error::{RayexecError, Result};
use std::task::Context;
use std::{sync::Arc, task::Waker};

use crate::execution::operators::util::hash::{hash_arrays, partition_for_hash};
use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};

use super::aggregate_hash_table::AggregateHashTable;
use super::grouping_set::GroupingSets;

#[derive(Debug)]
pub enum HashAggregatePartitionState {
    /// Partition is currently aggregating inputs.
    Aggregating {
        /// Output hash tables for storing aggregate states.
        ///
        /// There exists one hash table per output partition.
        output_hashtables: Vec<AggregateHashTable>,

        /// Reusable hashes buffer.
        hash_buf: Vec<u64>,

        /// Resusable partitions buffer.
        partitions_idx_buf: Vec<usize>,
    },

    /// Partition is currently producing final aggregate results.
    Producing {
        /// The aggregate hash table that we're pulling results from.
        hashtable: AggregateHashTable,
    },
}

#[derive(Debug)]
pub struct HashAggregateOperatorState {}

#[derive(Debug)]
pub struct PhysicalHashAggregate {
    /// Grouping sets we're grouping by.
    grouping_sets: GroupingSets,

    /// Columns we're computing aggregates for.
    aggregate_columns: Vec<usize>,
}

impl PhysicalOperator for PhysicalHashAggregate {
    fn poll_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::Aggregating {
                output_hashtables,
                hash_buf,
                partitions_idx_buf,
            } => {
                // Columns that we're computing the aggregate over.
                let aggregate_columns: Vec<_> = self
                    .aggregate_columns
                    .iter()
                    .map(|idx| {
                        batch
                            .column(*idx)
                            .expect("aggregate input column to exist")
                            .as_ref()
                    })
                    .collect();

                // Get the columns containg the "group" values (the columns in a
                // a GROUP BY).
                let grouping_columns: Vec<_> = self
                    .grouping_sets
                    .columns()
                    .iter()
                    .map(|idx| {
                        batch
                            .column(*idx)
                            .expect("grouping column to exist")
                            .as_ref()
                    })
                    .collect();

                let num_rows = batch.num_rows();
                hash_buf.resize(num_rows, 0);
                partitions_idx_buf.resize(num_rows, 0);

                let null_col = Array::Null(NullArray::new(num_rows));

                let mut masked_grouping_columns: Vec<&Array> =
                    Vec::with_capacity(grouping_columns.len());

                // For each mask, create a new set of grouping values, hash
                // them, and put into the hash maps.
                for null_mask in self.grouping_sets.null_masks() {
                    masked_grouping_columns.clear();

                    for (col_idx, col_is_null) in null_mask.iter().enumerate() {
                        if col_is_null {
                            masked_grouping_columns.push(&null_col);
                        } else {
                            masked_grouping_columns.push(grouping_columns[col_idx]);
                        }
                    }

                    // Compute hashes on the group by values.
                    let hashes = hash_arrays(&masked_grouping_columns, hash_buf)?;

                    // Compute _output_ partitions based on the hash values.
                    let num_partitions = output_hashtables.len();
                    for (partition, hash) in partitions_idx_buf.iter_mut().zip(hashes.iter()) {
                        *partition = partition_for_hash(*hash, num_partitions);
                    }

                    // For each partition, produce a selection bitmap, and
                    // insert the rows corresponding to that partition into the
                    // partition's hash table.
                    for partition_idx in 0..num_partitions {
                        // TODO: Could probably reuse bitmap allocations.
                        let selection = Bitmap::from_iter(
                            partitions_idx_buf.iter().map(|idx| *idx == partition_idx),
                        );

                        let partition_hashtable = &mut output_hashtables[partition_idx];
                        partition_hashtable.insert_groups(
                            &masked_grouping_columns,
                            hashes,
                            &aggregate_columns,
                            &selection,
                        )?;
                    }
                }

                // Aggregates don't produce anything until it's been finalized.
                Ok(PollPush::NeedsMore)
            }
            HashAggregatePartitionState::Producing { .. } => Err(RayexecError::new(
                "Attempted to push to partition that should be producing batches",
            )),
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
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::Producing { .. } => {
                unimplemented!()
            }
            HashAggregatePartitionState::Aggregating { .. } => Err(RayexecError::new(
                "Attempted to pull from partition that's still aggregating inputs",
            )),
        }
    }
}
