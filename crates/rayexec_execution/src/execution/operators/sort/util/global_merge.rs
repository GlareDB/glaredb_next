use rayexec_bullet::row::encoding::ComparableRow;
use rayexec_error::{RayexecError, Result};
use std::{cmp::Ordering, collections::BinaryHeap};

use super::sorted_batch::{PhysicallySortedBatch, SortedBatch};

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
    heap: BinaryHeap<PartitionRowReference<'a>>,

    /// Iterators for getting the next row in a batch.
    row_iters: Vec<SortedBatchIter<'a>>,

    /// Interleave indices buffer, (batch_idx, row_idx)
    ///
    /// Used to avoid reallocating everytime we generate new indices.
    indices_buf: Vec<(usize, usize)>,

    /// Row indices for each partition.
    ///
    /// These are updated as rows are popped from the heap.
    row_states: &'a mut [usize],
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
    /// Try to create a new k-way merger.
    ///
    /// `row_states` indicate the row index we should start at for each
    /// partition. These get updated as rows are popped from the heap.
    ///
    /// `batches` are the batches for each partition.
    pub fn try_new(
        row_states: &'a mut [usize],
        batches: &'a [Option<PhysicallySortedBatch>],
    ) -> Result<Self> {
        debug_assert_eq!(row_states.len(), batches.len());

        let mut iters: Vec<_> = row_states
            .iter()
            .zip(batches.iter())
            .enumerate()
            .filter_map(|(partition_idx, (row_start, batch))| match batch {
                Some(batch) => Some(SortedBatchIter {
                    partition_idx,
                    batch,
                    idx: *row_start,
                }),
                None => None,
            })
            .collect();

        let mut heap = BinaryHeap::with_capacity(iters.len());

        // Set up inital state. An empty iterator is means we have an empty
        // batch, which should have been removed prior to calling this.
        for iter in &mut iters {
            match iter.next() {
                Some(reference) => heap.push(reference),
                None => return Err(RayexecError::new("Unexpected empty iter")),
            }
        }

        Ok(GlobalKWayMerge {
            heap,
            row_iters: iters,
            row_states,
            indices_buf: Vec::new(),
        })
    }

    /// Compute the interleave indices that would produce a totally ordered
    /// batch from inputs.
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
                .push((reference.partition_idx, reference.row_idx));

            // Add next reference for this batch onto the heap.
            match self.row_iters[reference.partition_idx].next() {
                Some(next) => {
                    self.row_states[reference.partition_idx] += 1;
                    self.heap.push(next);
                }
                None => {
                    // Iterator exhausted, need to stop early so we can fetch
                    // the next batch for the partition.
                    return GlobalMergeResult::IterExhausted {
                        indices: &self.indices_buf,
                        input_idx: reference.partition_idx,
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
struct SortedBatchIter<'a> {
    partition_idx: usize,
    batch: &'a PhysicallySortedBatch,
    idx: usize,
}

impl<'a> Iterator for SortedBatchIter<'a> {
    type Item = PartitionRowReference<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let row_idx = self.idx;
        let row = self.batch.get_row(row_idx)?;
        self.idx += 1;

        Some(PartitionRowReference {
            partition_idx: self.partition_idx,
            row_idx,
            key: row,
        })
    }
}

/// A reference to row in a sorted batch for a partition.
///
/// `Eq` and `Ord` only take into account the sort key.
#[derive(Debug)]
struct PartitionRowReference<'a> {
    /// Index of the batch this reference is for.
    partition_idx: usize,

    /// Index of the row inside the batch this reference is for.
    row_idx: usize,

    /// The comparable row key itself.
    key: ComparableRow<'a>,
}

impl<'a> PartialEq for PartitionRowReference<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<'a> Eq for PartitionRowReference<'a> {}

impl<'a> PartialOrd for PartitionRowReference<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<'a> Ord for PartitionRowReference<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
