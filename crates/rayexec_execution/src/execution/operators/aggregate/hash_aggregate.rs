use parking_lot::Mutex;
use rayexec_bullet::array::{Array, NullArray};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::field::DataType;
use rayexec_error::{RayexecError, Result};
use std::collections::BTreeSet;
use std::fmt;
use std::task::{Context, Waker};

use crate::execution::operators::aggregate::aggregate_hash_table::AggregateStates;
use crate::execution::operators::util::hash::{hash_arrays, partition_for_hash};
use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};
use crate::expr::PhysicalAggregateExpression;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::aggregate_hash_table::{AggregateHashTableDrain, PartitionAggregateHashTable};
use super::grouping_set::GroupingSets;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashAggregateProjectionMapping {
    /// Projection indices, may include include columns produced by the
    /// aggregate, or columns that are being grouped in.
    ///
    /// Column indices less than `num_aggs` are outputs produced by the
    /// aggregate.
    ///
    /// Column indices greater or equal to `num_aggs` are grouping columns we
    /// can reference.
    pub projection: Vec<usize>,

    /// Number of aggregates we're computing.
    pub num_aggs: usize,
}

/// Used to specify the output of an aggregate operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAggregateColumnOutput {
    /// Reference a column that part of the grouping set.
    GroupingColumn(usize),

    /// Reference a computed aggregate result.
    AggregateResult(usize),
}

impl fmt::Display for HashAggregateColumnOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupingColumn(idx) => write!(f, "grouping_column({idx})"),
            Self::AggregateResult(idx) => write!(f, "result_column({idx})"),
        }
    }
}

#[derive(Debug)]
pub enum HashAggregatePartitionState {
    /// Partition is currently aggregating inputs.
    Aggregating {
        /// Index of this partition.
        partition_idx: usize,

        /// Output hash tables for storing aggregate states.
        ///
        /// There exists one hash table per output partition.
        output_hashtables: Vec<PartitionAggregateHashTable>,

        /// Reusable hashes buffer.
        hash_buf: Vec<u64>,

        /// Resusable partitions buffer.
        partitions_idx_buf: Vec<usize>,
    },

    /// Partition is currently producing final aggregate results.
    Producing {
        /// Index of this partition.
        partition_idx: usize,

        /// The aggregate hash table that we're pulling results from.
        ///
        /// May be None if the final hash table hasn't been built yet. If it
        /// hasn't been built, then the shared state will be need to be checked.
        hashtable_drain: Option<AggregateHashTableDrain>,
    },
}

impl HashAggregatePartitionState {
    fn partition_idx(&self) -> usize {
        match self {
            HashAggregatePartitionState::Aggregating { partition_idx, .. } => *partition_idx,
            HashAggregatePartitionState::Producing { partition_idx, .. } => *partition_idx,
        }
    }
}

#[derive(Debug)]
pub struct HashAggregateOperatorState {
    /// States containing pending hash tables from input partitions.
    output_states: Vec<Mutex<SharedOutputPartitionState>>,
}

#[derive(Debug)]
struct SharedOutputPartitionState {
    /// Completed hash tables from input partitions that should be combined into
    /// one final output table.
    completed: Vec<PartitionAggregateHashTable>,

    /// Number of remaining inputs. Initially set to number of input partitions.
    ///
    /// Once zero, the final hash table can be created.
    remaining: usize,

    /// Waker for thread that attempted to pull from this operator before we've
    /// completed the aggregation.
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalHashAggregate {
    /// Grouping sets we're grouping by.
    grouping_sets: GroupingSets,

    /// Datatypes of the columns in the grouping sets.
    group_types: Vec<DataType>,

    /// Union of all column indices that are inputs to the aggregate functions.
    aggregate_columns: Vec<usize>,

    /// How we should be outputting columns when pulling the completed batches.
    ///
    /// This projection can contain either grouping columns (columns specified
    /// in the GROUP BY), or the aggregate results themselves.
    projection: Vec<HashAggregateColumnOutput>,
}

impl PhysicalHashAggregate {
    pub fn try_new(
        num_partitions: usize,
        group_types: Vec<DataType>,
        grouping_sets: GroupingSets,
        exprs: Vec<PhysicalAggregateExpression>,
        projection: Vec<HashAggregateColumnOutput>,
    ) -> Result<(
        Self,
        HashAggregateOperatorState,
        Vec<HashAggregatePartitionState>,
    )> {
        // Collect all column indices that are part of computing the aggregate.
        let mut agg_input_cols = BTreeSet::new();
        for expr in &exprs {
            agg_input_cols.extend(expr.column_indices.iter().copied());
        }

        // Create column selection bitmaps for each aggregate expression. These
        // bitmaps are used to mask input columns into the operator.
        let mut col_selections = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            let col_selection = Bitmap::from_iter(
                agg_input_cols
                    .iter()
                    .map(|idx| expr.column_indices.contains(idx)),
            );
            col_selections.push(col_selection);
        }

        let operator_state = HashAggregateOperatorState {
            output_states: (0..num_partitions)
                .map(|_| {
                    Mutex::new(SharedOutputPartitionState {
                        completed: Vec::new(),
                        remaining: num_partitions,
                        pull_waker: None,
                    })
                })
                .collect(),
        };

        let mut partition_states = Vec::with_capacity(num_partitions);
        for idx in 0..num_partitions {
            let partition_local_tables = (0..num_partitions)
                .map(|_| {
                    let agg_states: Vec<_> = exprs
                        .iter()
                        .zip(col_selections.iter())
                        .map(|(expr, col_selection)| AggregateStates {
                            states: expr.function.new_grouped_state(),
                            col_selection: col_selection.clone(),
                        })
                        .collect();
                    PartitionAggregateHashTable::try_new(agg_states)
                })
                .collect::<Result<Vec<_>>>()?;

            let partition_state = HashAggregatePartitionState::Aggregating {
                partition_idx: idx,
                output_hashtables: partition_local_tables,
                hash_buf: Vec::new(),
                partitions_idx_buf: Vec::new(),
            };

            partition_states.push(partition_state);
        }

        let operator = PhysicalHashAggregate {
            group_types,
            grouping_sets,
            aggregate_columns: agg_input_cols.into_iter().collect(),
            projection,
        };

        Ok((operator, operator_state, partition_states))
    }
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
                ..
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
                    for (partition_idx, partition_hashtable) in
                        output_hashtables.iter_mut().enumerate()
                    {
                        // TODO: Could probably reuse bitmap allocations.
                        let selection = Bitmap::from_iter(
                            partitions_idx_buf.iter().map(|idx| *idx == partition_idx),
                        );

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
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::HashAggregate(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            state @ HashAggregatePartitionState::Aggregating { .. } => {
                // Set this partition's state to producing with an empty hash
                // table.
                //
                // On pull, this partition will build the final hash table from
                // the global state if all inputs are finished, or store a waker
                // if not.
                let producing_state = HashAggregatePartitionState::Producing {
                    partition_idx: state.partition_idx(),
                    hashtable_drain: None,
                };
                let aggregating_state = std::mem::replace(state, producing_state);
                let partition_hashtables = match aggregating_state {
                    HashAggregatePartitionState::Aggregating {
                        output_hashtables, ..
                    } => output_hashtables,
                    _ => unreachable!("state variant already checked in outer match"),
                };

                for (partition_idx, partition_hashtable) in
                    partition_hashtables.into_iter().enumerate()
                {
                    let mut output_state = operator_state.output_states[partition_idx].lock();
                    output_state.completed.push(partition_hashtable);

                    output_state.remaining -= 1;

                    // If we're the last input partition for an output
                    // partition, go ahead a wake up whoever is waiting.
                    if let Some(waker) = output_state.pull_waker.take() {
                        waker.wake();
                    }
                }

                Ok(())
            }
            HashAggregatePartitionState::Producing { .. } => Err(RayexecError::new(
                "Attempted to finalize a partition that's producing output",
            )),
        }
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

        let operator_state = match operator_state {
            OperatorState::HashAggregate(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::Producing {
                partition_idx,
                hashtable_drain,
            } => {
                // Check if we have the finaly hash table. Try to build it if we
                // don't.
                if hashtable_drain.is_none() {
                    let mut shared_state = operator_state.output_states[*partition_idx].lock();
                    if shared_state.remaining != 0 {
                        // Still need to wait for some input partitions to complete. Store our
                        // waker and come back later.
                        shared_state.pull_waker = Some(cx.waker().clone());
                        return Ok(PollPull::Pending);
                    }

                    // Othewise let's build the final table. Note that
                    // continuing to hold the lock here is fine since all inputs
                    // have completed and so won't try to acquire it.
                    let completed = std::mem::take(&mut shared_state.completed);
                    let mut completed_iter = completed.into_iter();
                    let mut first = completed_iter
                        .next()
                        .expect("there to be at least one partition");

                    for consume in completed_iter {
                        first.merge(consume)?;
                    }

                    let drain =
                        first.into_drain(1024, self.group_types.clone(), self.projection.clone()); // TODO: Make batch size configurable.
                    *hashtable_drain = Some(drain);
                }

                // Drain should be Some by here.
                match hashtable_drain.as_mut().unwrap().next() {
                    Some(Ok(batch)) => Ok(PollPull::Batch(batch)),
                    Some(Err(e)) => Err(e),
                    None => Ok(PollPull::Exhausted),
                }
            }
            HashAggregatePartitionState::Aggregating { partition_idx, .. } => {
                let mut shared = operator_state.output_states[*partition_idx].lock();
                shared.pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
        }
    }
}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        // TODO: grouping sets
        ExplainEntry::new("HashAggregate")
            .with_values("aggregate_columns", &self.aggregate_columns)
            .with_values("projection", &self.projection)
    }
}
