use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::collections::BinaryHeap;

use super::{accumulator::IndicesAccumulator, sorted_batch::RowReference};

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

    /// Still to initialize the iterator.
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

    /// If we're needing additional input.
    ///
    /// This is for debugging only.
    needs_input: bool,
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
            needs_input: false,
        }
    }

    pub fn num_inputs(&self) -> usize {
        self.row_reference_iters.len()
    }

    /// Push a batch and iterator for an input.
    ///
    /// This sets the iterator as initialized, and if all other iterators have
    /// been initialized, merging can proceed.
    pub fn push_batch_for_input(&mut self, input: usize, batch: Batch, iter: I) {
        self.needs_input = false;
        self.acc.push_input_batch(input, batch);
        self.row_reference_iters[input] = IterState::Iterator(iter);
    }

    /// Marks an input as finished.
    ///
    /// During merge, there will be no attempts to continue to read rows for
    /// this partition.
    pub fn input_finished(&mut self, input: usize) {
        self.needs_input = false;
        self.row_reference_iters[input] = IterState::Finished;
    }

    /// Try to merge the inputs, attempting to create a batch of size
    /// `batch_size`.
    ///
    /// If one of the inputs runs out of rows, the index of the input will be
    /// returned. `push_batch_for_input` or `input_finished` should be called
    /// before trying to continue the merge.
    pub fn try_merge(&mut self, batch_size: usize) -> Result<MergeResult> {
        let remaining = batch_size - self.acc.len();

        assert!(!self.needs_input, "Additional input needed");

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
                    self.needs_input = true;
                    return Ok(MergeResult::NeedsInput(reference.input_idx));
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
