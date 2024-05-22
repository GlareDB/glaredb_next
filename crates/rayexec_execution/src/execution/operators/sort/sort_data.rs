use rayexec_bullet::{
    array::Array,
    batch::Batch,
    compute,
    row::encoding::{ComparableRowEncoder, ComparableRows},
};
use rayexec_error::{RayexecError, Result};

use super::merge::KWayMerger;

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

#[derive(Debug)]
pub struct PartitionSortData {
    /// Columns we're ordering on.
    order_by: Vec<usize>,

    /// Encoder for getting keys from batches that can easily be compared.
    encoder: ComparableRowEncoder,

    /// Logically sorted batches.
    batches: Vec<KeySortedBatch>,
}

impl PartitionSortData {
    /// Push a batch into this partition's sort data.
    pub fn push_batch(&mut self, batch: Batch) -> Result<()> {
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
}

/// Holds a reference to a single column across all batches.
#[derive(Debug)]
struct ColumnAcrossBatches<'a> {
    columns: Vec<&'a Array>,
}

#[derive(Debug)]
pub struct BatchMerger<'a> {
    /// All batches represented as columns.
    columns: Vec<ColumnAcrossBatches<'a>>,

    /// Compute merge indices.
    merger: KWayMerger<'a>,
}

impl<'a> BatchMerger<'a> {
    pub fn next_batch(&mut self, batch_size: usize) -> Result<Option<Batch>> {
        let indices = match self.merger.next_interleave_indices(batch_size) {
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
