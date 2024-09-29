use hashbrown::raw::RawTable;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::fmt;

use super::condition::{HashJoinCondition, LeftPrecomputedJoinConditions};

/// Points to a row in the hash table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowKey {
    /// Index of the batch in the batches vector.
    pub batch_idx: u32,
    /// Index of the row in the batch.
    pub row_idx: u32,
}

pub struct PartitionHashTable {
    /// All collected batches.
    pub batches: Vec<Batch>,
    /// Conditions we're joining on.
    pub conditions: LeftPrecomputedJoinConditions,
    /// Hash table pointing to a row.
    pub hash_table: RawTable<(u64, RowKey)>,
}

impl PartitionHashTable {
    pub fn new(conditions: &[HashJoinCondition]) -> Self {
        let conditions = conditions.iter().map(|c| c.clone().into()).collect();

        PartitionHashTable {
            batches: Vec::new(),
            conditions: LeftPrecomputedJoinConditions { conditions },
            hash_table: RawTable::new(),
        }
    }

    /// Insert a batch into the hash table for the left side of the join.
    ///
    /// `hash_indices` indicates which columns in the batch was used to compute
    /// the hashes.
    pub fn insert_batch(&mut self, batch: Batch, hashes: &[u64]) -> Result<()> {
        assert_eq!(batch.num_rows(), hashes.len());

        self.conditions.precompute_for_left_batch(&batch)?;

        // Raw hashbrown reserves 1 at a time on insert if it's out of capacity.
        // Grow here if needed.
        //
        // TODO: We should initalize the hash table using underlying table
        // statistics.
        let remaining = self.hash_table.capacity() - self.hash_table.len();
        if remaining < batch.num_rows() {
            let additional = batch.num_rows() - remaining;
            self.hash_table.reserve(additional * 4, |(hash, _)| *hash);
        }

        let batch_idx = self.batches.len();
        self.batches.push(batch);

        for (row_idx, hash) in hashes.iter().enumerate() {
            let row_key = RowKey {
                batch_idx: batch_idx as u32,
                row_idx: row_idx as u32,
            };
            self.hash_table
                .insert(*hash, (*hash, row_key), |(hash, _)| *hash);
        }

        Ok(())
    }

    pub fn collected_batches(&self) -> &[Batch] {
        &self.batches
    }

    /// Merge some other hash table into this one.
    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        let batch_offset = self.batches.len();

        // Append all batches from other. When we drain the hash table, we'll
        // update the row keys to account for the new offset.
        self.batches.append(&mut other.batches);

        // Append all precompute left results.
        //
        // Similar to above, we just append precomputed results for each
        // condition which keeps the offset in sync.
        for (c1, c2) in self
            .conditions
            .conditions
            .iter_mut()
            .zip(other.conditions.conditions.iter_mut())
        {
            c1.left_precomputed.append(&mut c2.left_precomputed);
        }

        // Resize own hash table to reduce rehashing during the merge.
        let remaining = self.hash_table.capacity() - self.hash_table.len();
        if remaining < other.hash_table.len() {
            let additional = other.hash_table.len() - remaining;
            self.hash_table.reserve(additional, |(hash, _)| *hash);
        }

        for (hash, mut row_key) in other.hash_table.drain() {
            row_key.batch_idx += batch_offset as u32;
            self.hash_table
                .insert(hash, (hash, row_key), |(hash, _)| *hash);
        }

        Ok(())
    }
}

impl fmt::Debug for PartitionHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionHashTable").finish_non_exhaustive()
    }
}
