use rayexec_bullet::{
    batch::Batch,
    row::encoding::{ComparableRow, ComparableRows},
};

pub trait SortedBatch {
    fn get_row(&self, row_idx: usize) -> Option<ComparableRow>;
}

/// A batch that's been physically sorted.
///
/// Note that constructing this will not check that the batch is actually
/// sorted.
#[derive(Debug)]
pub struct PhysicallySortedBatch {
    /// The sorted batch.
    pub batch: Batch,

    /// The sorted keys.
    pub keys: ComparableRows,
}

impl SortedBatch for PhysicallySortedBatch {
    fn get_row(&self, row_idx: usize) -> Option<ComparableRow> {
        self.keys.row(row_idx)
    }
}

/// A logically sorted batch.
///
/// This doens't store a sorted batch itself, but instead stores row indices
/// which would result in a sorted batch.
///
/// Note that constructing this will not check that the indices actually lead to
/// a sorted batch.
#[derive(Debug)]
pub struct IndexSortedBatch {
    /// Indices of rows in sort order.
    pub sort_indices: Vec<usize>,

    /// Unsorted keys for the batch.
    pub keys: ComparableRows,

    /// The original unsorted batch.
    pub batch: Batch,
}

impl SortedBatch for IndexSortedBatch {
    fn get_row(&self, row_idx: usize) -> Option<ComparableRow> {
        let idx = self.sort_indices.get(row_idx)?;
        self.keys.row(*idx)
    }
}
