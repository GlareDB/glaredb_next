use rayexec_bullet::{
    batch::Batch,
    row::encoding::{ComparableRow, ComparableRows},
};
use rayexec_error::{RayexecError, Result};
use std::{cmp::Ordering, collections::BinaryHeap, rc::Rc};

use super::accumulator::IndicesAccumulator;

#[derive(Debug)]
pub struct KWayMerger {
    /// Accumulator for the interleave indices.
    acc: IndicesAccumulator,

    /// Heap containing the heads of all batches we're sorting.
    ///
    /// This heap contains at most one row reference for each batch. This row
    /// reference indicates the "head" of the sorted batch. When a row reference
    /// is popped, the next row reference for that same batch should be pushed
    /// onto the heap.
    heap: BinaryHeap<RowReference>,
}

impl KWayMerger {
    pub fn try_merge(&mut self, batch_size: usize) -> Result<Batch> {
        unimplemented!()
    }
}

/// A reference to row in a sorted batch.
///
/// The `Ord` and `Eq` implementations only takes into account the row key, and
/// not the batch index or row index. This lets us shove these references into a
/// heap containing references to multiple batches, letting us getting the total
/// order of all batches.
#[derive(Debug)]
struct RowReference {
    /// Index of the batch this reference is for.
    batch_idx: usize,

    /// Index of the row inside the batch this reference is for.
    row_idx: usize,

    /// Reference to the comparable rows.
    rows: Rc<ComparableRows>,
}

impl RowReference {
    fn row(&self) -> ComparableRow {
        self.rows.row(self.row_idx).expect("row to exist")
    }
}

impl PartialEq for RowReference {
    fn eq(&self, other: &Self) -> bool {
        self.row() == other.row()
    }
}

impl Eq for RowReference {}

impl PartialOrd for RowReference {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.row().partial_cmp(&other.row())
    }
}

impl Ord for RowReference {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row().cmp(&other.row())
    }
}
