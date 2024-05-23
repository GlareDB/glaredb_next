use std::collections::VecDeque;

use rayexec_bullet::{
    array::Array,
    batch::Batch,
    compute,
    row::encoding::{ComparableColumn, ComparableRow, ComparableRowEncoder, ComparableRows},
};
use rayexec_error::{RayexecError, Result};

use crate::{execution::operators::sort::merge::RowReferenceIter, expr::PhysicalSortExpression};

use super::merge::KWayMerger;

#[derive(Debug)]
pub struct BatchAndKeys {
    batch: Batch,
    keys: ComparableRows,
}

/// A logically sorted batch.
///
/// Note that this doens't store a sorted batch itself, but instead stores row
/// indices which would result in a sorted batch.
#[derive(Debug)]
pub struct KeySortedBatch {
    /// Indices of rows in sort order.
    pub sort_indices: Vec<usize>,

    /// Unsorted keys for the batch.
    pub keys: ComparableRows,

    /// The original unsorted batch.
    pub batch: Batch,
}

/// Holds working data for partition-local sorting.
#[derive(Debug)]
pub struct PartitionWorkingSortData {
    /// Columns we're ordering on.
    order_by: Vec<usize>,

    /// Encoder for getting keys from batches that can easily be compared.
    encoder: ComparableRowEncoder,

    /// Logically sorted batches.
    batches: Vec<KeySortedBatch>,
}

impl PartitionWorkingSortData {
    pub fn new(exprs: &[PhysicalSortExpression]) -> Self {
        let order_by = exprs.iter().map(|expr| expr.column).collect();
        let encoder = ComparableRowEncoder {
            columns: exprs
                .iter()
                .map(|expr| ComparableColumn {
                    desc: expr.desc,
                    nulls_first: expr.nulls_first,
                })
                .collect(),
        };

        PartitionWorkingSortData {
            order_by,
            encoder,
            batches: Vec::new(),
        }
    }

    /// Push a batch into this partition's sort data.
    pub fn push_batch(&mut self, batch: Batch) -> Result<()> {
        let sort_cols = self.sort_columns(&batch)?;
        let sort_rows = self.encoder.encode(&sort_cols)?;

        // Produce the indices that would result in a sorted batches. We can use
        // these indices later to `take` rows once we want to start returning
        // sorted batches.
        let mut sort_indices: Vec<_> = (0..batch.num_rows()).collect();
        sort_indices.sort_by_key(|idx| sort_rows.row(*idx).expect("row to exist"));

        let key_batch = KeySortedBatch {
            sort_indices,
            keys: sort_rows,
            batch,
        };

        self.batches.push(key_batch);

        Ok(())
    }

    /// Try to totally sort all batches collected so far.
    ///
    /// This does not update the internal state of `self`.
    pub fn try_into_total_sort(&self, batch_size: usize) -> Result<PartitionTotalSortData> {
        let num_cols = match self.batches.first() {
            Some(batch) => batch.batch.columns().len(),
            None => {
                // No batches is valid.
                return Ok(PartitionTotalSortData {
                    batches: VecDeque::new(),
                });
            }
        };

        // Pull out all columns to make interleaving easier.
        let mut cols: Vec<ColumnAcrossBatches> = Vec::with_capacity(num_cols);
        for idx in 0..num_cols {
            let columns = self
                .batches
                .iter()
                .map(|batch| batch.batch.column(idx).expect("column to exist").as_ref())
                .collect();
            cols.push(ColumnAcrossBatches { columns })
        }

        // Create row iters for each batch, working in sorted order.
        let iters: Vec<_> = self
            .batches
            .iter()
            .enumerate()
            .map(|(idx, batch)| RowReferenceIter::new(idx, &batch.sort_indices, &batch.keys))
            .collect();

        let k_way = KWayMerger::new(iters);
        let merger = BatchMerger {
            columns: cols,
            merger: k_way,
            batch_size,
        };

        let sorted = merger.collect::<Result<VecDeque<_>>>()?;

        Ok(PartitionTotalSortData { batches: sorted })
    }

    /// Get the sort columns for a batch.
    fn sort_columns<'a>(&self, batch: &'a Batch) -> Result<Vec<&'a Array>> {
        let sort_cols = self
            .order_by
            .iter()
            .map(|idx| {
                batch
                    .column(*idx)
                    .map(|col| col.as_ref())
                    .ok_or_else(|| RayexecError::new("Missing column"))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(sort_cols)
    }
}

#[derive(Debug)]
pub struct PartitionTotalSortData {
    /// Totally sorted batches.
    ///
    /// batches[0].last_row >= batches[1].first_row, batches[1].last_row >= batches[2].first_row, etc
    batches: VecDeque<Batch>,
}

impl Iterator for PartitionTotalSortData {
    type Item = Batch;
    fn next(&mut self) -> Option<Self::Item> {
        self.batches.pop_front()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.batches.len();
        (len, Some(len))
    }
}

/// Holds a reference to a single column across all batches.
#[derive(Debug)]
struct ColumnAcrossBatches<'a> {
    columns: Vec<&'a Array>,
}

#[derive(Debug)]
struct BatchMerger<'a> {
    /// All batches represented as columns.
    columns: Vec<ColumnAcrossBatches<'a>>,

    /// Compute merge indices.
    merger: KWayMerger<'a>,

    /// Desired batch size.
    batch_size: usize,
}

impl<'a> BatchMerger<'a> {
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        let indices = match self.merger.next_interleave_indices(self.batch_size) {
            Some(indices) => indices,
            None => return Ok(None),
        };

        // Build the batch using the computed indices.
        let mut merged_columns = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let merged = compute::interleave::interleave(&column.columns, indices)?;
            merged_columns.push(merged);
        }

        let batch = Batch::try_new(merged_columns)?;

        Ok(Some(batch))
    }
}

impl<'a> Iterator for BatchMerger<'a> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}
