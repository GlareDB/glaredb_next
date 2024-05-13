use crate::functions::aggregate::GroupedStates;
use hashbrown::raw::RawTable;
use rayexec_bullet::{
    array::{Array, NullArray},
    batch::Batch,
    bitmap::Bitmap,
    row::{OwnedRow, Row},
};
use rayexec_error::{RayexecError, Result};
use std::fmt;

use super::hash_aggregate::HashAggregateColumnOutput;

/// States for a single aggregation.
#[derive(Debug)]
pub struct AggregateStates {
    pub states: Box<dyn GroupedStates>,

    /// Bitmap for selecting columns from the input to the hash map.
    ///
    /// This is used to allow the hash map to handle states for different
    /// aggregates working on different columns. For example:
    ///
    /// SELECT SUM(a), MIN(b) FROM ...
    ///
    /// This query computes aggregates on columns 'a' and 'b', but to minimize
    /// work, we pass both 'a' and 'b' to the hash table in one pass. Then this
    /// bitmap is used to further refine the inputs specific to the aggregate.
    pub col_selection: Bitmap,
}

/// An aggregate hash table for storing group values alongside the computed
/// aggregates.
///
/// This can be used to store partial aggregate data for a single partition and
/// later combined other other hash tables also containing partial aggregate
/// data for the same partition.
pub struct PartitionAggregateHashTable {
    /// Statest for aggregates.
    ///
    /// There should exist one `AggregateState` per aggregate function call.
    ///
    /// - `SELECT SUM(a), ...` => length of 1
    /// - `SELECT SUM(a), MAX(b), ...` => length  of 2
    agg_states: Vec<AggregateStates>,

    // TODO: This is likely a peformance bottleneck with storing group values in
    // rows.
    group_values: Vec<OwnedRow>,

    /// Hash table pointing to the group index.
    hash_table: RawTable<(u64, usize)>,

    /// Buffer used when looking for group indices for group values.
    indexes_buffer: Vec<usize>,
}

impl PartitionAggregateHashTable {
    /// Create a new hash table using the provided aggregate states.
    ///
    /// All states must have zero initialized states.
    pub fn try_new(agg_states: Vec<AggregateStates>) -> Result<Self> {
        for agg in &agg_states {
            if agg.states.num_groups() != 0 {
                return Err(RayexecError::new(format!(
                    "Attempted to initialize aggregate table with non-empty states: {agg:?}"
                )));
            }
        }

        Ok(PartitionAggregateHashTable {
            agg_states,
            group_values: Vec::new(),
            hash_table: RawTable::new(),
            indexes_buffer: Vec::new(),
        })
    }

    pub fn insert_groups(
        &mut self,
        groups: &[&Array],
        hashes: &[u64],
        inputs: &[&Array],
        selection: &Bitmap,
    ) -> Result<()> {
        let row_count = selection.popcnt();
        // If none of the rows are actually selection for insertion into the hash map, then
        // we don't need to do anything.
        if row_count == 0 {
            return Ok(());
        }

        self.indexes_buffer.clear();
        self.indexes_buffer.reserve(row_count);

        // Get group indices, creating new states as needed for groups we've
        // never seen before.
        self.find_or_create_group_indices(groups, hashes, selection)?;

        // Now we just rip through the values.
        for agg_states in self.agg_states.iter_mut() {
            let input_cols: Vec<_> = agg_states
                .col_selection
                .iter()
                .zip(inputs.iter())
                .filter_map(|(selected, arr)| if selected { Some(*arr) } else { None })
                .collect();

            agg_states
                .states
                .update_from_arrays(&input_cols, &self.indexes_buffer)?;
        }

        Ok(())
    }

    fn find_or_create_group_indices(
        &mut self,
        groups: &[&Array],
        hashes: &[u64],
        selection: &Bitmap,
    ) -> Result<()> {
        for (row_idx, (&hash, selected)) in hashes.iter().zip(selection.iter()).enumerate() {
            if !selected {
                continue;
            }

            // TODO: This is probably a bit slower than we'd want.
            //
            // It's like that replacing this with something that compares
            // scalars directly to a arrays at an index would be faster.
            let row = Row::try_new_from_arrays(groups, row_idx)?;

            // Look up the entry into the hash table.
            let ent = self.hash_table.get_mut(hash, |(_hash, group_idx)| {
                row == self.group_values[*group_idx]
            });

            match ent {
                Some((_, group_idx)) => {
                    // Group already exists.
                    self.indexes_buffer.push(*group_idx);
                }
                None => {
                    // Need to create new states and insert them into the hash table.
                    let mut states_iter = self.agg_states.iter_mut();

                    // Use first state to generate the group index. Each new
                    // state we create for this group should generate the same
                    // index.
                    let group_idx = match states_iter.next() {
                        Some(agg_state) => agg_state.states.new_group(),
                        None => {
                            return Err(RayexecError::new("Aggregate hash table has no aggregates"))
                        }
                    };

                    for agg_state in states_iter {
                        let idx = agg_state.states.new_group();
                        // Very critical, if we're not generating the same
                        // index, all bets are off.
                        assert_eq!(group_idx, idx);
                    }

                    self.hash_table
                        .insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);

                    self.group_values.push(row.into_owned());
                    self.indexes_buffer.push(group_idx);
                }
            }
        }

        debug_assert_eq!(hashes.len(), self.indexes_buffer.len());

        Ok(())
    }

    /// Merge other hash table into self.
    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        let row_count = other.group_values.len();
        if row_count == 0 {
            return Ok(());
        }

        self.indexes_buffer.clear();
        self.indexes_buffer.reserve(row_count);

        // Ensure the has table we're merging into has all the groups from
        // the other hash table.
        for (hash, group_idx) in other.hash_table.drain() {
            // TODO: Deduplicate with othe find and create method.

            let row = std::mem::replace(&mut other.group_values[group_idx], Row::empty());

            let ent = self.hash_table.get_mut(hash, |(_hash, self_group_idx)| {
                &row == &self.group_values[*self_group_idx]
            });

            match ent {
                Some((_, self_group_idx)) => {
                    // 'self' already has the group from the other table.
                    self.indexes_buffer.push(*self_group_idx)
                }
                None => {
                    // 'self' has never seend this group before. Add it to the map with
                    // an empty state.

                    let mut states_iter = self.agg_states.iter_mut();

                    // Use first state to generate the group index. Each new
                    // state we create for this group should generate the same
                    // index.
                    let group_idx = match states_iter.next() {
                        Some(agg_state) => agg_state.states.new_group(),
                        None => {
                            return Err(RayexecError::new("Aggregate hash table has no aggregates"))
                        }
                    };

                    for agg_state in states_iter {
                        let idx = agg_state.states.new_group();
                        // Very critical, if we're not generating the same
                        // index, all bets are off.
                        assert_eq!(group_idx, idx);
                    }

                    self.hash_table
                        .insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);

                    self.group_values.push(row.into_owned());
                    self.indexes_buffer.push(group_idx);
                }
            }
        }

        // And now we combine the states using the computed mappings.
        let other_states = std::mem::take(&mut other.agg_states);
        for (own_state, other_state) in self.agg_states.iter_mut().zip(other_states.into_iter()) {
            own_state
                .states
                .try_combine(other_state.states, &self.indexes_buffer)?;
        }

        Ok(())
    }

    pub fn into_drain(
        self,
        batch_size: usize,
        projection: Vec<HashAggregateColumnOutput>,
    ) -> AggregateHashTableDrain {
        AggregateHashTableDrain {
            projection,
            batch_size,
            table: self,
        }
    }
}

impl fmt::Debug for PartitionAggregateHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateHashTable")
            .field("aggregate_states", &self.agg_states)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct AggregateHashTableDrain {
    projection: Vec<HashAggregateColumnOutput>,
    batch_size: usize,
    table: PartitionAggregateHashTable,
}

impl AggregateHashTableDrain {
    fn next_inner(&mut self) -> Result<Option<Batch>> {
        let cols = self
            .table
            .agg_states
            .iter_mut()
            .map(|agg_state| agg_state.states.drain_finalize_n(self.batch_size))
            .collect::<Result<Option<Vec<_>>>>()?;
        let mut cols = match cols {
            Some(cols) => cols,
            None => return Ok(None),
        };

        // TODO: Use actual group values, this is currently just a placeholder.
        let group_width = self
            .table
            .group_values
            .first()
            .map(|row| row.columns.len())
            .unwrap_or(0);
        let len = cols.first().map(|arr| arr.len()).unwrap_or(0);
        let mut group_cols: Vec<_> = (0..group_width)
            .map(|_| Array::Null(NullArray::new(len)))
            .collect();

        let num_result_cols = cols.len();
        cols.append(&mut group_cols);

        // Batch column ordering has aggregate results first, following by the
        // grouping columns.
        let batch = Batch::try_new(cols)?;

        // Get projection indices based on the above.
        let project_indices = self
            .projection
            .iter()
            .map(|proj| match *proj {
                HashAggregateColumnOutput::GroupingColumn(idx) => idx + num_result_cols,
                HashAggregateColumnOutput::AggregateResult(idx) => idx,
            })
            .collect::<Vec<_>>();

        let projected = batch.project(&project_indices);

        Ok(Some(projected))
    }
}

impl Iterator for AggregateHashTableDrain {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}
