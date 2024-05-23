use rayexec_bullet::batch::Batch;
use rayexec_bullet::row::encoding::{ComparableRow, ComparableRows};
use rayexec_error::{RayexecError, Result};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::sorted_batch::{IndexSortedBatch, PhysicallySortedBatch, SortedBatch};

/// Produce indices for merging k number of batches into totally ordered output
/// batches.
///
/// This is used when merging multiple batches within a partition to produce the
/// partition's output. As such, this will continue to run until all iterators
/// producing row references are exhausted.
#[derive(Debug)]
pub struct LocalKWayMerge<'a> {
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
    row_iters: Vec<SortedBatchIter<'a, IndexSortedBatch>>,

    /// Interleave indices buffer, (batch_idx, row_idx)
    ///
    /// Used to avoid reallocating everytime we generate new indices.
    indices_buf: Vec<(usize, usize)>,
}

impl<'a> LocalKWayMerge<'a> {
    pub fn new(mut row_iters: Vec<SortedBatchIter<'a, IndexSortedBatch>>) -> Self {
        let mut heap = BinaryHeap::with_capacity(row_iters.len());

        // Fill up initial heap state.
        for iter in &mut row_iters {
            if let Some(reference) = iter.next() {
                heap.push(reference);
            }
        }

        LocalKWayMerge {
            heap,
            row_iters,
            indices_buf: Vec::new(),
        }
    }

    /// Get the next set of interleave indices which would produce a totally
    /// ordered batch.
    pub fn interleave_indices(&mut self, max_batch_size: usize) -> Option<&[(usize, usize)]> {
        self.indices_buf.reserve(max_batch_size);
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
            if let Some(next) = self.row_iters[reference.batch_idx].next() {
                self.heap.push(next);
            }

            self.indices_buf
                .push((reference.batch_idx, reference.row_idx))
        }

        Some(&self.indices_buf)
    }
}

/// Produces indices for merging k number of batches into totally ordered output
/// batches.
///
/// Similar to `LocalKWayMerge` except for use with merged batches across
/// partitions. The primary difference is each iterator provided represents the
/// input batches from one partition. And when an iterator is exhausted, we
/// break early instead of exhausting all iterators so that we can go and fetch
/// the next batch for that partition.
///
/// Since we need to keep the state around in order to "resume" merging once we
/// fetch the next batch for a partition, it's easier to just have this be a
/// separate type from `LocalKWayMerge` instead of trying to abstract over it.
#[derive(Debug)]
pub struct GlobalKWayMerge<'a> {
    /// Heap containing the heads of all batches we're sorting.
    heap: BinaryHeap<RowReference<'a>>,

    /// Iterators for getting the next row in a batch.
    row_iters: Vec<SortedBatchIter<'a, PhysicallySortedBatch>>,

    /// Interleave indices buffer, (batch_idx, row_idx)
    ///
    /// Used to avoid reallocating everytime we generate new indices.
    indices_buf: Vec<(usize, usize)>,

    /// Input states into the merge.
    inputs: &'a mut [GlobalKWayMergeInputStates],
}

/// Input into the global merge.
#[derive(Debug)]
pub struct GlobalKWayMergeInputStates {
    /// Batch input.
    ///
    /// If None, an iterator for this input will not be created.
    batch: Option<PhysicallySortedBatch>,

    /// Index of the row to start scanning from.
    row_start: usize,
}

impl GlobalKWayMergeInputStates {
    pub fn replace_batch(&mut self, batch: PhysicallySortedBatch) {
        self.batch = Some(batch);
        self.row_start = 0;
    }

    pub fn batch_is_none(&self) -> bool {
        self.batch.is_none()
    }

    pub fn take_batch(&mut self) -> Option<PhysicallySortedBatch> {
        self.batch.take()
    }
}

#[derive(Debug)]
pub enum GlobalMergeResult<'a> {
    /// Some number of indices produced. Either the max number of indices were
    /// computed (according to max batch size), or the heap has been exhausted.
    /// If the heap's been exhausted, the next round of merging will produce an
    /// `Exhausted` result.
    ///
    /// Nothing need to happen prior to the next call to `interleave_indices`.
    Indices { indices: &'a [(usize, usize)] },

    /// One of the iterators were exhausted.
    ///
    /// Prior to the next call to `interleave_indices`, the merge input as
    /// indicated by `input_idx` needs to have a new batch inserted.
    IterExhausted {
        indices: &'a [(usize, usize)],
        input_idx: usize,
    },

    /// There's no more rows that will be produced. This indicates the global
    /// sort is done.
    Exhausted,
}

impl<'a> GlobalKWayMerge<'a> {
    pub fn interleave_indices(&mut self, max_batch_size: usize) -> GlobalMergeResult {
        self.indices_buf.reserve(max_batch_size);
        self.indices_buf.clear();

        for _idx in 0..max_batch_size {
            // TODO: If the heap only contains a single row reference, we know
            // that there's only one batch we'll be pulling from. We should just
            // short circuit in that case.

            let reference = match self.heap.pop() {
                Some(r) => r,
                None => break, // Heap empty, we're done.
            };

            self.indices_buf
                .push((reference.batch_idx, reference.row_idx));

            // Add next reference for this batch onto the heap.
            match self.row_iters[reference.batch_idx].next() {
                Some(next) => {
                    self.inputs[reference.batch_idx].row_start += 1;
                    self.heap.push(next);
                }
                None => {
                    // Iterator exhausted, need to stop early so we can fetch
                    // the next batch for the partition.
                    return GlobalMergeResult::IterExhausted {
                        indices: &self.indices_buf,
                        input_idx: reference.batch_idx,
                    };
                }
            }
        }

        if self.indices_buf.is_empty() {
            GlobalMergeResult::Exhausted
        } else {
            GlobalMergeResult::Indices {
                indices: &self.indices_buf,
            }
        }
    }
}

#[derive(Debug)]
struct SortedBatchIter<'a, B: SortedBatch> {
    batch_idx: usize,
    batch: &'a B,
    idx: usize,
}

impl<'a, B: SortedBatch> Iterator for SortedBatchIter<'a, B> {
    type Item = RowReference<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let row_idx = self.idx;
        let row = self.batch.get_row(row_idx)?;
        self.idx += 1;

        Some(RowReference {
            batch_idx: self.batch_idx,
            row_idx,
            key: row,
        })
    }
}

/// A reference to row in a sorted batch.
///
/// The `Ord` and `Eq` implementations only takes into account the row key, and
/// not the batch index or row index. This lets us shove these references into a
/// heap containing references to multiple batches, letting us getting the total
/// order of all batches.
#[derive(Debug)]
struct RowReference<'a> {
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
