use rayexec_bullet::batch::Batch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug)]
pub struct SortExpr {
    /// Column index to order by.
    column: usize,

    /// Ascending or descending
    order: SortOrder,

    /// Where to place nulls.
    nulls: NullsOrder,
}

/// A logically sorted batch.
#[derive(Debug)]
pub struct KeySortedBatch {
    /// Indices of rows in sort order.
    rows: Vec<usize>,

    /// The original unsorted batch.
    batch: Batch,
}

#[derive(Debug)]
pub struct PartitionSortData {
    /// Sort expressions we're ordering by.
    order_by: Vec<SortExpr>,

    /// Logically sorted batches.
    batches: Vec<KeySortedBatch>,

    /// Desired output batch size.
    batch_size: usize,
}
