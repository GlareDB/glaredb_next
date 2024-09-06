use hashbrown::raw::RawTable;
use rayexec_bullet::{batch::Batch, datatype::DataType};
use rayexec_error::Result;

use super::condition::LeftPrecomputedJoinConditions;

/// Points to a row in the hash table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowKey {
    /// Index of the batch in the batches vector.
    batch_idx: usize,
    /// Index of the row in the batch.
    row_idx: usize,
}

pub struct JoinHashTable {
    /// All collected batches.
    batches: Vec<Batch>,
    /// Conditions we're joining on.
    conditions: LeftPrecomputedJoinConditions,
    /// Hash table pointing to a row.
    hash_table: RawTable<(u64, RowKey)>,
    /// Column types for left side of join.
    left_types: Vec<DataType>,
    /// Column types for right side of join.
    right_types: Vec<DataType>,
}

impl JoinHashTable {
    /// Insert a batch into the hash table for the left side of the join.
    ///
    /// `hash_indices` indicates which columns in the batch was used to compute
    /// the hashes.
    pub fn insert_batch(&mut self, batch: Batch, hashes: &[u64]) -> Result<()> {
        assert_eq!(batch.num_rows(), hashes.len());

        self.conditions.precompute_for_left_batch(&batch)?;

        let batch_idx = self.batches.len();
        self.batches.push(batch);

        for (row_idx, hash) in hashes.iter().enumerate() {
            let row_key = RowKey { batch_idx, row_idx };
            self.hash_table
                .insert(*hash, (*hash, row_key), |(hash, _)| *hash);
        }

        Ok(())
    }

    /// Merge some other hash table into this one.
    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        let batch_offset = self.batches.len();

        // Append all batches from other. When we drain the hash table, we'll
        // update the row keys to account for the new offset.
        self.batches.append(&mut other.batches);

        // Append all precompute left results.
        //
        // Similar to above, we just append to the end for each condition which
        // keeps the offset in sync.
        for (c1, c2) in self
            .conditions
            .conditions
            .iter_mut()
            .zip(other.conditions.conditions.iter_mut())
        {
            c1.left_precomputed.append(&mut c2.left_precomputed);
        }

        for (hash, mut row_key) in other.hash_table.drain() {
            row_key.batch_idx += batch_offset;
            self.hash_table
                .insert(hash, (hash, row_key), |(hash, _)| *hash);
        }

        Ok(())
    }
}
