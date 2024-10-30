use std::fmt;

use hashbrown::raw::RawTable;
use rayexec_bullet::array::Array;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::Result;

const NOT_YET_INSERTED_CHUNK: u32 = u32::MAX - 1;

pub struct AggregateHashTable {
    table: RawTable<(u64, RowAddress)>,
    append_buffers: AppendBuffers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowAddress {
    chunk: u32,
    row: u32,
}

#[derive(Debug)]
struct TableChunk {}

/// Various reusable buffers.
#[derive(Debug)]
struct AppendBuffers {
    /// Rows the need to be compared.
    ///
    /// We checked that theres a hash in the hash table, but we still need to do
    /// a full equality check.
    needs_compare: Vec<usize>,
}

impl AggregateHashTable {
    pub fn insert(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn find_or_create_groups(&mut self, groups: &[Array], hashes: &[u64]) -> Result<()> {
        self.append_buffers.needs_compare.clear();
        self.append_buffers.needs_compare.resize(hashes.len(), 0);

        let mut new_group_count = 0;

        for (row_idx, hash) in hashes.iter().enumerate() {
            if self.table.get(*hash, |(h, _)| h == hash).is_some() {
                // Hash is in the table. Mark this row as needing to be
                // compared.
                self.append_buffers.needs_compare.push(row_idx);
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

                new_group_count += 1;
            }
        }

        // If we've inserted new group hashes, go ahead and create the actual
        // groups.

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
