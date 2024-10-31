use std::collections::BTreeSet;
use std::sync::Arc;

use rayexec_bullet::array::Array;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::{RayexecError, Result};

use super::aggregate_hash_table::Aggregate;
use super::chunk::GroupChunk;
use super::drain::HashTableDrain;
use super::entry::EntryKey;
use crate::execution::operators::hash_aggregate::compare::group_values_eq;

const LOAD_FACTOR: f64 = 0.75;

/// Aggregate hash table.
#[derive(Debug)]
pub struct HashTable {
    /// All chunks in the table.
    pub(crate) chunks: Vec<GroupChunk>,
    pub(crate) entries: Vec<EntryKey<GroupAddress>>,
    pub(crate) num_occupied: usize,
    pub(crate) insert_buffers: InsertBuffers,
    pub(crate) aggregates: Vec<Aggregate>,
}

/// Address to a single group in the hash table.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GroupAddress {
    pub chunk_idx: u32,
    pub row_idx: u32,
}

impl GroupAddress {
    const fn empty() -> Self {
        GroupAddress {
            chunk_idx: 0,
            row_idx: 0,
        }
    }
}

/// Reusable buffers during hash table inserts.
#[derive(Debug, Default)]
pub(crate) struct InsertBuffers {
    /// Computed offsets into entries.
    offsets: Vec<usize>,
    /// Selection vector containing indices for inputs rows that still need to
    /// be inserted into the table.
    needs_insert: SelectionVector,
    /// Selection vector pointing to new groups.
    new_group_rows: SelectionVector,
    /// Selection vector pointing to rows that need to be compared.
    needs_compare: SelectionVector,
    /// Rows that don't pass the equality check.
    not_eq_rows: SelectionVector,
    /// Group addresses for each row in the input.
    group_addresses: Vec<GroupAddress>,
    /// Chunks we'll be inserting into.
    // TODO: Try to remove this.
    chunk_indices: BTreeSet<u32>,
}

impl HashTable {
    pub fn new(capacity: usize, aggregates: Vec<Aggregate>) -> Self {
        HashTable {
            chunks: Vec::new(),
            entries: vec![EntryKey::default(); capacity],
            num_occupied: 0,
            insert_buffers: InsertBuffers::default(),
            aggregates,
        }
    }

    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    pub fn insert(&mut self, groups: &[Array], hashes: &[u64], inputs: &[Array]) -> Result<()> {
        // Find and create groups as needed.
        self.find_or_create_groups(groups, hashes)?;

        // Now update aggregate states.
        //
        // We iterate the addresses to figure out which chunks actually need
        // upating.
        self.insert_buffers.chunk_indices.clear();
        self.insert_buffers.chunk_indices.extend(
            self.insert_buffers
                .group_addresses
                .iter()
                .map(|addr| addr.chunk_idx),
        );

        for &chunk_idx in &self.insert_buffers.chunk_indices {
            let chunk = &mut self.chunks[chunk_idx as usize];
            chunk.update_states(inputs, &self.insert_buffers.group_addresses)?;
        }

        Ok(())
    }

    pub fn merge(&mut self, other: &mut HashTable) -> Result<()> {
        for mut other_chunk in other.chunks.drain(..) {
            // Find or create groups in self from other.
            self.find_or_create_groups(&other_chunk.arrays, &other_chunk.hashes)?;

            // Now figure out which chunks we need to update in self. Find or
            // create groups would have already created new chunks with empty
            // states for us for groups we haven't seen in self.
            self.insert_buffers.chunk_indices.clear();
            self.insert_buffers.chunk_indices.extend(
                self.insert_buffers
                    .group_addresses
                    .iter()
                    .map(|addr| addr.chunk_idx),
            );

            for &chunk_idx in &self.insert_buffers.chunk_indices {
                let chunk = &mut self.chunks[chunk_idx as usize];
                chunk.combine_states(&mut other_chunk, &self.insert_buffers.group_addresses)?;
            }
        }

        Ok(())
    }

    pub fn into_drain(self) -> HashTableDrain {
        HashTableDrain {
            table: self,
            drain_idx: 0,
        }
    }

    fn find_or_create_groups(&mut self, groups: &[Array], hashes: &[u64]) -> Result<()> {
        let num_inputs = hashes.len();

        if num_inputs == 0 {
            return Ok(());
        }

        // Resize addresses, this will be where we store all the group
        // addresses that will be used during the state update.
        //
        // Existing values don't matter, they'll be overwritten as we update the
        // table.
        self.insert_buffers
            .group_addresses
            .resize(num_inputs, GroupAddress::default());

        // Check to see if we should resize. Typically not all groups will
        // create a new entry, but it's possible so we need to account for that.
        self.resize_if_needed(num_inputs)?;

        // Precompute offsets into the table.
        self.insert_buffers.offsets.clear();
        self.insert_buffers.offsets.resize(num_inputs, 0);
        let cap = self.capacity() as u64;
        for (idx, &hash) in hashes.iter().enumerate() {
            self.insert_buffers.offsets[idx] = (hash % cap) as usize;
        }

        // Init selection to all rows in input.
        self.insert_buffers.needs_insert.clear();
        self.insert_buffers
            .needs_insert
            .append_locations(0..num_inputs);

        let mut remaining = num_inputs;

        // Number of new groups we've created.
        let mut new_groups = 0;

        while remaining > 0 {
            // Pushed to as we occupy new entries.
            self.insert_buffers.new_group_rows.clear();
            // Pushed to as we find rows that need to be compared.
            self.insert_buffers.needs_compare.clear();
            // Pushed to during the equality check when hashes match.
            self.insert_buffers.not_eq_rows.clear();

            // Figure out where we're putting remaining rows.
            for idx in 0..remaining {
                let row_idx = self.insert_buffers.needs_insert.get_unchecked(idx);
                let offset = &mut self.insert_buffers.offsets[row_idx];

                // Probe
                let mut iter_count = 0;
                loop {
                    let ent = &mut self.entries[*offset];

                    if ent.is_empty() {
                        // Empty entry, claim it.
                        //
                        // Sets the prefix, but inserts an empty group address.
                        // The real group address will be figured out during
                        // state initalization.
                        *ent = EntryKey::new(hashes[row_idx], GroupAddress::empty());
                        self.insert_buffers.new_group_rows.push_location(row_idx);
                        new_groups += 1;
                        break;
                    }

                    // Entry not empty...

                    // Check if hash prefix matches. If it does, we need to mark
                    // for comparison. If it doesn't we have linear probe.
                    if ent.hash == hashes[row_idx] {
                        self.insert_buffers.needs_compare.push_location(row_idx);
                        break;
                    }

                    // Otherwise need to increment.
                    *offset = ((*offset + 1) as u64 % cap) as usize;

                    if iter_count > cap {
                        // We wrapped. This shouldn't happen during normal
                        // execution as the hash table should've been resized to
                        // fit everything.
                        //
                        // But Sean writes bugs, so just in case...
                        return Err(RayexecError::new("Hash table completely full"));
                    }
                    iter_count += 1;
                }
            }

            // If we've inserted new group hashes, go ahead and create the actual
            // groups.
            if !self.insert_buffers.new_group_rows.is_empty() {
                // TODO: Try not to clone?
                let selection = Arc::new(self.insert_buffers.new_group_rows.clone());

                let group_vals: Vec<_> = groups
                    .iter()
                    .map(|a| {
                        let mut arr = a.clone();
                        arr.select_mut(selection.clone());
                        arr
                    })
                    .collect();

                let num_new_groups = self.insert_buffers.new_group_rows.len();

                // TODO: Try to append to previous chunk if < desired batch size.
                let chunk_idx = self.chunks.len();
                let mut states: Vec<_> =
                    self.aggregates.iter().map(|agg| agg.new_states()).collect();

                // Initialize the states.
                for _ in 0..num_new_groups {
                    states.iter_mut().for_each(|state| {
                        let _ = state.states.new_group();
                    });
                }

                let chunk = GroupChunk {
                    chunk_idx: chunk_idx as u32,
                    num_groups: num_new_groups,
                    hashes: self
                        .insert_buffers
                        .new_group_rows
                        .iter_locations()
                        .map(|loc| hashes[loc])
                        .collect(),
                    arrays: group_vals,
                    aggregate_states: states,
                };
                self.chunks.push(chunk);

                // Update hash table entries to point to the new chunk.
                //
                // Accounts for the selection we did when putting the arrays
                // into the chunk.
                for (updated_idx, row_idx) in self
                    .insert_buffers
                    .new_group_rows
                    .iter_locations()
                    .enumerate()
                {
                    let offset = self.insert_buffers.offsets[row_idx];
                    let ent = &mut self.entries[offset];

                    let addr = GroupAddress {
                        chunk_idx: chunk_idx as u32,
                        row_idx: updated_idx as u32,
                    };

                    *ent = EntryKey::new(hashes[row_idx], addr);

                    // Update output addresses too.
                    self.insert_buffers.group_addresses[row_idx] = addr;
                }
            }

            // We have rows to compare.
            if !self.insert_buffers.needs_compare.is_empty() {
                // Update addresses slice with the groups we'll be comparing
                // against.
                for row_idx in self.insert_buffers.needs_compare.iter_locations() {
                    let offset = self.insert_buffers.offsets[row_idx];
                    let ent = &self.entries[offset];
                    // Sets address for this row to existing group. If the rows
                    // are actually equal, then this remains as is. Otherwise
                    // the next iteration(s) of the loop will update this to
                    // keep trying to compare.
                    self.insert_buffers.group_addresses[row_idx] = ent.key;
                }

                // Compare our input groups to the existing groups.
                self.insert_buffers.needs_insert.clear();
                group_values_eq(
                    groups,
                    hashes,
                    &self.insert_buffers.needs_compare,
                    &self.chunks,
                    &self.insert_buffers.group_addresses,
                    &mut self.insert_buffers.not_eq_rows,
                )?;
            }

            // Now for every row that failed the equality check, increment its
            // offset to try the next entry in the table.
            for row_idx in self.insert_buffers.not_eq_rows.iter_locations() {
                let offset = &mut self.insert_buffers.offsets[row_idx];
                *offset = ((*offset + 1) as u64 % cap) as usize;
            }

            // No try next iteration just with rows that failed the equality
            // check.
            std::mem::swap(
                &mut self.insert_buffers.needs_insert,
                &mut self.insert_buffers.not_eq_rows,
            );

            remaining = self.insert_buffers.needs_insert.len();
        }

        self.num_occupied += new_groups;

        Ok(())
    }

    fn resize(&mut self, new_capacity: usize) -> Result<()> {
        if new_capacity < self.entries.len() {
            return Err(RayexecError::new("Cannot reduce capacity"));
        }

        let mut new_entries = vec![EntryKey::default(); new_capacity];

        for ent in self.entries.drain(..) {
            let mut offset = ent.hash as usize % new_capacity;

            // Keep looping until we find an empty entry.
            while !new_entries[offset].is_empty() {
                offset += 1;
                if offset >= new_capacity {
                    offset = 0;
                }
            }

            new_entries[offset] = ent;
        }

        self.entries = new_entries;

        Ok(())
    }

    /// Resize the hash table if needed, keeping the load factor less than
    /// LOAD_FACTOR.
    fn resize_if_needed(&mut self, num_inputs: usize) -> Result<()> {
        let possible_occupied = num_inputs + self.num_occupied;

        // Calculate the minimum required capacity.
        let mut new_capacity = self.capacity();
        while (possible_occupied as f64) / (new_capacity as f64) >= LOAD_FACTOR {
            new_capacity *= 2;
        }

        // Now resize if needed.
        if new_capacity != self.capacity() {
            self.resize(new_capacity)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::bitmap::Bitmap;
    use rayexec_bullet::datatype::DataType;

    use super::*;
    use crate::functions::aggregate::sum::Sum;
    use crate::functions::aggregate::AggregateFunction;

    fn make_hash_table() -> HashTable {
        let aggregate = Aggregate {
            function: Sum.plan_from_datatypes(&[DataType::Int64]).unwrap(),
            col_selection: Bitmap::from_iter([true]),
        };

        HashTable::new(16, vec![aggregate])
    }

    #[test]
    fn insert_simple() {
        let groups = [Array::from_iter(["g1", "g2", "g1"])];
        let inputs = [Array::from_iter::<[i64; 3]>([1, 2, 3])];

        let hashes = [4, 5, 4]; // Hashes for group values.

        let mut table = make_hash_table();
        table.insert(&groups, &hashes, &inputs).unwrap();

        assert_eq!(2, table.num_occupied);
    }

    #[test]
    fn insert_hash_collision() {
        let groups = [Array::from_iter(["g1", "g2", "g1"])];
        let inputs = [Array::from_iter::<[i64; 3]>([1, 2, 3])];

        let hashes = [4, 4, 4];

        let mut table = make_hash_table();
        table.insert(&groups, &hashes, &inputs).unwrap();

        assert_eq!(2, table.num_occupied);
    }

    #[test]
    fn insert_require_resize() {
        // 17 unique groups (> initial 16 capacity)

        let groups = [Array::from_iter(0..17)];
        let inputs = [Array::from_iter(0 as i64..17 as i64)];

        let hashes = vec![44; 17]; // All hashes collide.

        let mut table = make_hash_table();
        table.insert(&groups, &hashes, &inputs).unwrap();

        assert_eq!(17, table.num_occupied);
    }

    #[test]
    fn insert_require_resize_more_than_double() {
        // 33 unique groups, more than twice initial capacity. Caught bug where
        // resize by doubling didn't increase capacity enough.

        let groups = [Array::from_iter(0..33)];
        let inputs = [Array::from_iter(0 as i64..33 as i64)];

        let hashes = vec![44; 33]; // All hashes collide.

        let mut table = make_hash_table();
        table.insert(&groups, &hashes, &inputs).unwrap();

        assert_eq!(33, table.num_occupied);
    }
}
