use hashbrown::raw::RawTable;
use rayexec_bullet::{batch::Batch, bitmap::Bitmap, compute, datatype::DataType};
use rayexec_error::Result;
use std::{collections::HashMap, fmt};

use crate::execution::operators::util::outer_join_tracker::{
    LeftOuterJoinTracker, RightOuterJoinTracker,
};

use super::{
    condition::{HashJoinCondition, LeftPrecomputedJoinCondition, LeftPrecomputedJoinConditions},
    partition_hash_table::{PartitionHashTable, RowKey},
};

pub struct GlobalHashTable {
    /// All collected batches.
    batches: Vec<Batch>,
    /// Conditions we're joining on.
    conditions: LeftPrecomputedJoinConditions,
    /// Hash table pointing to a row.
    hash_table: RawTable<(u64, RowKey)>,
    /// Column types for left side of join.
    ///
    /// Used when generating the left columns for a RIGHT OUTER join.
    left_types: Vec<DataType>,
    /// If we're a right join.
    right_join: bool,
    /// If we're a mark join.
    ///
    /// If true, we won't actually be doing any joining, and instead just update
    /// the visit bitmaps.
    is_mark: bool,
}

impl GlobalHashTable {
    /// Merge many partition hash tables into a new global hash table.
    pub fn new(
        left_types: Vec<DataType>,
        right_join: bool,
        is_mark: bool,
        partition_tables: Vec<PartitionHashTable>,
        conditions: &[HashJoinCondition],
    ) -> Self {
        // Merge all partition tables left to right.

        let batches_cap: usize = partition_tables.iter().map(|t| t.batches.len()).sum();
        let hash_table_cap: usize = partition_tables.iter().map(|t| t.hash_table.len()).sum();
        let precomputed_cap: usize = partition_tables
            .iter()
            .map(|t| {
                t.conditions
                    .conditions
                    .iter()
                    .map(|c| c.left_precomputed.len())
                    .sum::<usize>()
            })
            .sum();

        let mut batches = Vec::with_capacity(batches_cap);
        let mut hash_table = RawTable::with_capacity(hash_table_cap);

        let mut conditions = LeftPrecomputedJoinConditions {
            conditions: conditions
                .iter()
                .map(|c| {
                    LeftPrecomputedJoinCondition::from_condition_with_capacity(
                        c.clone(),
                        precomputed_cap,
                    )
                })
                .collect(),
        };

        for mut table in partition_tables {
            let batch_offset = batches.len();

            // Merge batches.
            batches.append(&mut table.batches);

            // Merge hash tables, updating row key to point to the correct batch
            // in the merged batch vec.
            for (hash, mut row_key) in table.hash_table.drain() {
                row_key.batch_idx += batch_offset as u32;
                hash_table.insert(hash, (hash, row_key), |(hash, _)| *hash);
            }

            // Append all precompute left results.
            //
            // We just append precomputed results for each condition which keeps
            // the offset in sync.
            for (c1, c2) in conditions
                .conditions
                .iter_mut()
                .zip(table.conditions.conditions.iter_mut())
            {
                c1.left_precomputed.append(&mut c2.left_precomputed);
            }
        }

        GlobalHashTable {
            batches,
            conditions,
            hash_table,
            left_types,
            right_join,
            is_mark,
        }
    }

    pub fn collected_batches(&self) -> &[Batch] {
        &self.batches
    }

    /// Probe the table.
    pub fn probe(
        &self,
        right: &Batch,
        selection: Option<&Bitmap>,
        hashes: &[u64],
        mut left_outer_tracker: Option<&mut LeftOuterJoinTracker>,
    ) -> Result<Vec<Batch>> {
        // Track per-batch row indices that match the input columns.
        //
        // The value is a vec of (left_idx, right_idx) pairs pointing to rows in
        // the left (build) and right (probe) batches respectively
        let mut row_indices: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();

        for (right_idx, hash) in hashes.iter().enumerate() {
            // Get all matching row keys from hash table.
            //
            // SAFETY: Iterator only lives for this method call.
            // See: https://docs.rs/hashbrown/latest/hashbrown/raw/struct.RawTable.html#method.iter_hash
            unsafe {
                self.hash_table.iter_hash(*hash).for_each(|bucket| {
                    let val = bucket.as_ref(); // Unsafe
                    let row_key = val.1;

                    // Hashbrown only stores first seven bits of hash. We check
                    // here to further prune items we pull out of the table.
                    //
                    // Note this still doesn't guarantee row equality. That is
                    // checked when we actually execute the conditions, this
                    // just gets us the candidates.
                    if &val.0 != hash {
                        return;
                    }

                    // This is all safe, just adding to the row_indices vec.
                    use std::collections::hash_map::Entry;
                    match row_indices.entry(row_key.batch_idx as usize) {
                        Entry::Occupied(mut ent) => {
                            ent.get_mut().push((row_key.row_idx as usize, right_idx))
                        }
                        Entry::Vacant(ent) => {
                            ent.insert(vec![(row_key.row_idx as usize, right_idx)]);
                        }
                    }
                })
            }
        }

        let mut right_tracker = if self.right_join {
            Some(RightOuterJoinTracker::new_for_batch(right, selection))
        } else {
            None
        };

        let mut batches = Vec::with_capacity(row_indices.len());
        for (batch_idx, row_indices) in row_indices {
            let (left_rows, right_rows): (Vec<_>, Vec<_>) = row_indices.into_iter().unzip();

            // Update right unvisited bitmap. May be None if we're not doing a
            // RIGHT JOIN.
            if let Some(right_outer_tracker) = right_tracker.as_mut() {
                right_outer_tracker.mark_rows_visited(&right_rows);
            }

            // Initial right side of the batch.
            let initial_right_side = Batch::try_new2(
                right
                    .columns2()
                    .iter()
                    .map(|arr| compute::take::take(arr.as_ref(), &right_rows))
                    .collect::<Result<Vec<_>>>()?,
            )?;

            // Run through conditions. This will also check the column equality
            // for the join key (since it's just another condition).
            let selection = self.conditions.compute_selection_for_probe(
                batch_idx,
                &left_rows,
                &initial_right_side,
            )?;

            // Prune left row indices using selection.
            let left_rows: Vec<_> = left_rows
                .into_iter()
                .zip(selection.iter())
                .filter_map(|(left_row, selected)| if selected { Some(left_row) } else { None })
                .collect();

            // Update left visit bitmaps with rows we're visiting from batches
            // in the hash table.
            //
            // This is done _after_ evaluating the join conditions which may
            // result in fewer rows on the left that we're actually joining
            // with.
            //
            // May be None if we're not doing a LEFT JOIN.
            if let Some(left_outer_tracker) = left_outer_tracker.as_mut() {
                left_outer_tracker
                    .mark_rows_visited_for_batch(batch_idx, left_rows.iter().copied());
            }

            // Don't actually do the join.
            if self.is_mark {
                // But continue working on the next batch.
                continue;
            }

            // Get the left columns for this batch.
            let left_batch = self.batches.get(batch_idx).expect("batch to exist");
            let left_cols = left_batch
                .columns2()
                .iter()
                .map(|arr| compute::take::take(arr.as_ref(), &left_rows))
                .collect::<Result<Vec<_>>>()?;

            // Trim down right cols using only selected rows.
            let right_cols = initial_right_side
                .columns2()
                .iter()
                .map(|arr| compute::filter::filter(arr, &selection))
                .collect::<Result<Vec<_>>>()?;

            // Create final batch.
            let batch = Batch::try_new2(left_cols.into_iter().chain(right_cols))?;
            batches.push(batch);
        }

        // Append batch from RIGHT OUTER if needed.
        if let Some(right_tracker) = right_tracker {
            let extra = right_tracker.into_unvisited(&self.left_types, right)?;
            batches.push(extra);
        }

        Ok(batches)
    }
}

impl fmt::Debug for GlobalHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlobalHashTable").finish_non_exhaustive()
    }
}
