use std::fmt;
use std::sync::Arc;

use hashbrown::raw::RawTable;
use rayexec_bullet::array::Array;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::{RayexecError, Result};

use crate::functions::aggregate::{GroupedStates, PlannedAggregateFunction};

const NOT_YET_INSERTED_CHUNK: u32 = u32::MAX - 1;

#[derive(Debug)]
pub struct Aggregate {
    pub function: Box<dyn PlannedAggregateFunction>,
    pub col_selection: Bitmap,
}

impl Aggregate {
    pub fn new_states(&self) -> AggregateStates {
        AggregateStates {
            states: self.function.new_grouped_state(),
            col_selection: self.col_selection.clone(),
        }
    }
}

/// States for a single aggregation.
#[derive(Debug)]
pub struct AggregateStates {
    /// The states we're tracking for a single aggregate.
    ///
    /// Internally the state are stored in a vector, with the index of the
    /// vector corresponding to the index of the group in the table's
    /// `group_values` vector.
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

pub struct AggregateHashTable {
    aggregates: Vec<Aggregate>,
    table: RawTable<(u64, RowAddress)>,
    append_buffers: AppendBuffers,
    chunks: Vec<TableChunk>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowAddress {
    chunk: u32,
    row: u32,
}

#[derive(Debug)]
struct TableChunk {
    num_groups: usize,
    columns: Vec<Array>,
    states: Vec<AggregateStates>,
}

/// Various reusable buffers.
#[derive(Debug)]
struct AppendBuffers {
    /// Rows the need to be compared.
    ///
    /// We checked that theres a hash in the hash table, but we still need to do
    /// a full equality check.
    needs_compare_rows: Vec<usize>,
    new_group_rows: Vec<usize>,
    new_group_hashes: Vec<u64>,
}

impl AggregateHashTable {
    pub fn insert(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn find_or_create_groups(&mut self, groups: &[Array], hashes: &[u64]) -> Result<()> {
        self.append_buffers.needs_compare_rows.clear();
        self.append_buffers
            .needs_compare_rows
            .resize(hashes.len(), 0);

        self.append_buffers.new_group_rows.clear();
        self.append_buffers.new_group_rows.resize(hashes.len(), 0);

        self.append_buffers.new_group_hashes.clear();
        self.append_buffers.new_group_hashes.resize(hashes.len(), 0);

        let mut remaining = hashes.len();

        while remaining > 0 {
            for (row_idx, hash) in hashes.iter().enumerate() {
                if self.table.get(*hash, |(h, _)| h == hash).is_some() {
                    // Hash is in the table. Mark this row as needing to be
                    // compared.
                    self.append_buffers.needs_compare_rows.push(row_idx);
                } else {
                    // Hash not in table, we're for sure going to be inserting this
                    // row as a group.
                    self.table.insert(
                        *hash,
                        (
                            *hash,
                            RowAddress {
                                chunk: NOT_YET_INSERTED_CHUNK,
                                row: row_idx as u32,
                            },
                        ),
                        |(hash, _)| *hash,
                    );

                    self.append_buffers.new_group_rows.push(row_idx);
                }
            }

            // If we've inserted new group hashes, go ahead and create the actual
            // groups.
            if !self.append_buffers.new_group_rows.is_empty() {
                // TODO: Try not to clone?
                let selection = Arc::new(SelectionVector::from(
                    self.append_buffers.new_group_rows.clone(),
                ));

                let group_vals: Vec<_> = groups
                    .iter()
                    .map(|a| {
                        let mut arr = a.clone();
                        arr.select_mut(selection.clone());
                        arr
                    })
                    .collect();

                let num_groups = self.append_buffers.new_group_rows.len();

                // TODO: Try to append to previous chunk if < desired batch size.
                let chunk_idx = self.chunks.len();
                let mut states: Vec<_> =
                    self.aggregates.iter().map(|agg| agg.new_states()).collect();

                // Initialize the states.
                for _ in 0..num_groups {
                    states.iter_mut().for_each(|state| {
                        let _ = state.states.new_group();
                    });
                }

                let chunk = TableChunk {
                    num_groups,
                    columns: group_vals,
                    states,
                };
                self.chunks.push(chunk);

                // Update addresses in hash table to new address referencing the
                // create chunk and row within that chunk.
                for (new_row_idx, (&old_row_idx, &hash)) in self
                    .append_buffers
                    .new_group_rows
                    .iter()
                    .zip(&self.append_buffers.new_group_hashes)
                    .enumerate()
                {
                    let addr = self
                        .table
                        .get_mut(hash, |(_, addr)| {
                            addr == &RowAddress {
                                chunk: NOT_YET_INSERTED_CHUNK,
                                row: old_row_idx as u32,
                            }
                        })
                        .ok_or_else(|| RayexecError::new("Missing old address"))?;

                    addr.1 = RowAddress {
                        chunk: chunk_idx as u32,
                        row: new_row_idx as u32,
                    }
                }
            }

            // If we have rows to compare, go ahead and compare them.
            if !self.append_buffers.needs_compare_rows.is_empty() {}
        }

        unimplemented!()
    }

    fn append_group_data(&mut self, groupes: &[Array], sel: SelectionVector) -> Result<()> {
        unimplemented!()
    }
}

impl fmt::Debug for AggregateHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateHashTable").finish_non_exhaustive()
    }
}
