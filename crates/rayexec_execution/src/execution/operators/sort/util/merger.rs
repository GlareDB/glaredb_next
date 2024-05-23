use rayexec_bullet::{
    batch::Batch,
    row::encoding::{ComparableRow, ComparableRows},
};
use rayexec_error::{RayexecError, Result};
use std::{cmp::Ordering, collections::BinaryHeap, rc::Rc};

use super::accumulator::IndicesAccumulator;

#[derive(Debug)]
pub enum MergeResult {
    /// We have a merged batch.
    ///
    /// Nothing else needed before the next call to `try_merge`.
    Batch(Batch),

    /// Need to push a new batch for the input at the given index.
    ///
    /// `push_batch_for_input` should be called before the next call to
    /// `try_merge`.
    NeedsInput(usize),

    /// No more outputs will be produced.
    Exhausted,
}

#[derive(Debug)]
enum IterState<I> {
    /// Normal state, we just need to iterate.
    Iterator(I),

    /// Still to initialize the iterator. If we reach this state when attempting
    /// to merge, we'll error. That indicates a programmer bug.
    NeedsInitialize,

    /// Input is finished, we don't need to account for this iterator anymore.
    Finished,
}

/// Merge k inputs into totally sorted outputs.
#[derive(Debug)]
pub struct KWayMerger<I> {
    /// Accumulator for the interleave indices.
    acc: IndicesAccumulator,

    /// Heap containing the heads of all batches we're sorting.
    ///
    /// This heap contains at most one row reference for each batch. This row
    /// reference indicates the "head" of the sorted batch. When a row reference
    /// is popped, the next row reference for that same batch should be pushed
    /// onto the heap.
    heap: BinaryHeap<RowReference>,

    /// Iterators for getting row references. This iterator should return rows
    /// in order.
    ///
    /// Indexed by input idx.
    ///
    /// None indicates no more batches for the input.
    row_reference_iters: Vec<IterState<I>>,
}

impl<I> KWayMerger<I>
where
    I: Iterator<Item = RowReference>,
{
    pub fn new(num_inputs: usize) -> Self {
        KWayMerger {
            acc: IndicesAccumulator::new(num_inputs),
            heap: BinaryHeap::with_capacity(num_inputs),
            row_reference_iters: (0..num_inputs)
                .map(|_| IterState::NeedsInitialize)
                .collect(),
        }
    }

    /// Push a batch and iterator for an input.
    ///
    /// This sets the iterator as initialized, and if all other iterators have
    /// been initialized, merging can proceed.
    pub fn push_batch_for_input(&mut self, input: usize, batch: Batch, iter: I) {
        self.acc.push_input_batch(input, batch);
        self.row_reference_iters[input] = IterState::Iterator(iter);
    }

    /// Marks an input as finished.
    ///
    /// During merge, there will be no attempts to continue to read rows for
    /// this partition.
    pub fn input_finished(&mut self, input: usize) {
        self.row_reference_iters[input] = IterState::Finished;
    }

    /// Try to merge the inputs, attempting to create a batch of size
    /// `batch_size`.
    ///
    /// If one of the inputs runs out of rows, the index of the input will be
    /// returned. `push_batch_for_input` or `input_finished` needs to be called
    /// before trying to continue the merge, otherwise no progress will be made.
    pub fn try_merge(&mut self, batch_size: usize) -> Result<MergeResult> {
        let remaining = batch_size - self.acc.len();

        for _ in 0..remaining {
            // TODO: If the heap only contains a single row reference, we know
            // that there's only one batch we'll be pulling from. We should just
            // short circuit in that case.

            let reference = match self.heap.pop() {
                Some(r) => r,
                None => break, // Heap empty, we're done. Break and try to build.
            };

            self.acc.append_row_to_indices(reference.input_idx);

            match &mut self.row_reference_iters[reference.input_idx] {
                IterState::Iterator(iter) => match iter.next() {
                    Some(reference) => self.heap.push(reference),
                    None => return Ok(MergeResult::NeedsInput(reference.input_idx)),
                },
                IterState::NeedsInitialize => {
                    return Err(RayexecError::new("Reached uninitialized iterator"))
                }
                IterState::Finished => (), // Just continue. No more batches from this input.
            }
        }

        match self.acc.build()? {
            Some(batch) => Ok(MergeResult::Batch(batch)),
            None => Ok(MergeResult::Exhausted),
        }
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
    /// Index of the input this reference is for.
    input_idx: usize,

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
