use rayexec_bullet::row::encoding::{ComparableRow, ComparableRows};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Computes merge indices from k number of batches.
///
/// This returns computed indices which should be used to interleave batches
/// together to form totally ordered batches.
#[derive(Debug)]
pub struct KWayMerger<'a> {
    /// Heap containing the heads of all batches we're sorting.
    ///
    /// This heap contains at most one row reference for each batch. This row
    /// reference indicates the "head" of the sorted batch. When a row reference
    /// is popped, the next row reference for that same batch should be pushed
    /// onto the heap.
    heap: BinaryHeap<RowReference<'a>>,

    /// Iterators for getting the next row in a batch.
    ///
    /// Length of this should equal the number of batches we're merging.
    row_iters: Vec<RowReferenceIter<'a>>,

    /// Interleave indices buffer, (batch_idx, row_idx)
    ///
    /// Used to avoid reallocating everytime we generate new indices.
    indices_buf: Vec<(usize, usize)>,
}

impl<'a> KWayMerger<'a> {
    pub fn new(mut row_iters: Vec<RowReferenceIter<'a>>) -> Self {
        let mut heap = BinaryHeap::with_capacity(row_iters.len());

        // Fill up initial heap state.
        for iter in &mut row_iters {
            if let Some(reference) = iter.next() {
                heap.push(reference);
            }
        }

        KWayMerger {
            heap,
            row_iters,
            indices_buf: Vec::new(),
        }
    }

    /// Get the next set of interleave indices which would produce a totally
    /// ordered batch.
    pub fn next_interleave_indices(&mut self, max_batch_size: usize) -> Option<&[(usize, usize)]> {
        self.indices_buf.clear();

        for _idx in 0..max_batch_size {
            // TODO: If the heap only contains a single row reference, we know
            // that there's only one batch we'll be pulling from. We should just
            // short circuit in that case.

            let reference = match self.heap.pop() {
                Some(r) => r,
                None => break, // Heap empty, we're done.
            };

            // Add next reference for this batch onto the heap.
            if let Some(next_reference) = self.row_iters[reference.batch_idx].next() {
                self.heap.push(next_reference);
            }

            self.indices_buf
                .push((reference.batch_idx, reference.row_idx))
        }

        Some(&self.indices_buf)
    }
}

#[derive(Debug)]
pub struct RowReferenceIter<'a> {
    batch_idx: usize,
    sort_idx: usize,
    sort_indices: &'a [usize],
    rows: &'a ComparableRows,
}

impl<'a> Iterator for RowReferenceIter<'a> {
    type Item = RowReference<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sort_idx >= self.sort_indices.len() {
            return None;
        }

        let row_idx = self.sort_indices[self.sort_idx];
        let row = self.rows.row(row_idx).expect("row to exist");

        self.sort_idx += 1;

        Some(RowReference {
            batch_idx: self.batch_idx,
            row_idx,
            key: row,
        })
    }
}

/// A reference to row in a partition's sort data.
///
/// The `Ord` and `Eq` implementations only takes into account the row key, and
/// not the batch index or row index. This lets us shove these references into a
/// heap containing references to multiple batches, letting us getting the total
/// order of all batches.
#[derive(Debug)]
pub struct RowReference<'a> {
    /// Index of the batch this reference is for.
    batch_idx: usize,

    /// Index of the row inside the batch this reference is for.
    row_idx: usize,

    /// The comparable row key itself.
    key: ComparableRow<'a>,
}

impl<'a> PartialEq for RowReference<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<'a> Eq for RowReference<'a> {}

impl<'a> PartialOrd for RowReference<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<'a> Ord for RowReference<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
