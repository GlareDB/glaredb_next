use crate::functions::aggregate::{GroupedStates, SpecializedAggregateFunction};
use hashbrown::raw::RawTable;
use rayexec_bullet::{
    array::Array,
    bitmap::{self, Bitmap},
    row::{OwnedRow, Row},
};
use rayexec_error::{RayexecError, Result};
use std::fmt;

/// An aggregate hash table for storing group values alongside the computed
/// aggregates.
///
/// This can be used to store partial aggregate data for a single partition and
/// later combined other other hash tables also containing partial aggregate
/// data for the same partition.
pub struct PartitionAggregateHashTable {
    /// Statest for aggregates.
    ///
    /// There should exist one `GroupedStates` per aggregate function call.
    ///
    /// - `SELECT SUM(a), ...` => length of 1
    /// - `SELECT SUM(a), MAX(b), ...` => length  of 2
    aggregate_states: Vec<Box<dyn GroupedStates>>,

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
    pub fn try_new(aggregate_states: Vec<Box<dyn GroupedStates>>) -> Result<Self> {
        for state in &aggregate_states {
            if state.num_groups() != 0 {
                return Err(RayexecError::new(format!(
                    "Attempted to initialize aggregate table with non-empty states: {state:?}"
                )));
            }
        }

        Ok(PartitionAggregateHashTable {
            aggregate_states,
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
        for states in self.aggregate_states.iter_mut() {
            states.update_from_arrays(inputs, &self.indexes_buffer)?;
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
                    let mut states_iter = self.aggregate_states.iter_mut();

                    // Use first state to generate the group index. Each new
                    // state we create for this group should generate the same
                    // index.
                    let group_idx = match states_iter.next() {
                        Some(state) => state.new_group(),
                        None => {
                            return Err(RayexecError::new("Aggregate hash table has no aggregates"))
                        }
                    };

                    for state in states_iter {
                        let idx = state.new_group();
                        // Very critical, if we're not generating the same
                        // index, all bets are off.
                        assert_eq!(group_idx, idx);
                    }

                    self.hash_table
                        .insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);

                    self.indexes_buffer.push(group_idx);
                }
            }
        }

        debug_assert_eq!(hashes.len(), self.indexes_buffer.len());

        Ok(())
    }

    /// Merge other hash tables into self.
    pub fn merge(&mut self, mut others: Vec<Self>) -> Result<()> {
        for other in others.iter_mut() {
            let row_count = other.group_values.len();
            if row_count == 0 {
                continue;
            }

            self.indexes_buffer.clear();
            self.indexes_buffer.reserve(row_count);

            // Ensure the has table we're merging into has all the groups from
            // the other hash table.
            for (hash, group_idx) in other.hash_table.drain() {
                // TODO: Deduplicate with othe find and create method.
                let ent = self.hash_table.get_mut(hash, |(_hash, self_group_idx)| {
                    &other.group_values[group_idx] == &self.group_values[*self_group_idx]
                });

                match ent {
                    Some((_, self_group_idx)) => {
                        // 'self' already has the group from the other table.
                        self.indexes_buffer.push(*self_group_idx)
                    }
                    None => {
                        // 'self' has never seend this group before. Add it to the map with
                        // an empty state.

                        let mut states_iter = self.aggregate_states.iter_mut();

                        // Use first state to generate the group index. Each new
                        // state we create for this group should generate the same
                        // index.
                        let group_idx = match states_iter.next() {
                            Some(state) => state.new_group(),
                            None => {
                                return Err(RayexecError::new(
                                    "Aggregate hash table has no aggregates",
                                ))
                            }
                        };

                        for state in states_iter {
                            let idx = state.new_group();
                            // Very critical, if we're not generating the same
                            // index, all bets are off.
                            assert_eq!(group_idx, idx);
                        }

                        self.hash_table
                            .insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);

                        self.indexes_buffer.push(group_idx);
                    }
                }
            }

            //
        }
        unimplemented!()
    }
}

impl fmt::Debug for PartitionAggregateHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateHashTable")
            .field("aggregate_states", &self.aggregate_states)
            .finish_non_exhaustive()
    }
}
