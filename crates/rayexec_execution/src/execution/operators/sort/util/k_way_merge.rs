use rayexec_bullet::batch::Batch;
use rayexec_bullet::row::encoding::{ComparableRow, ComparableRows};
use rayexec_error::{RayexecError, Result};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A sorted batch with its associated sort keys.
#[derive(Debug)]
pub struct SortedBatch {
    pub batch: Batch,
    pub keys: ComparableRows,
}

/// Storeable state for restarting a k-way merge.
///
/// This is used when merging sorted partition inputs into a single partition.
/// Since inputs may not be available at the time of the pull, we need a way to
/// store the state between executions.
#[derive(Debug)]
pub struct RestartableKWayState {
    /// States that are used to initialize the k-way merger.
    ///
    /// Essentially this stores the row index to start at when iterator over a
    /// sorted batch.
    ///
    /// As input partitions are exhausted, states will be set to None.
    states: Vec<Option<BatchState>>,
}

impl RestartableKWayState {
    /// Create a new state for some number of partitions.
    pub fn new(num_partitions: usize) -> Self {
        RestartableKWayState {
            states: (0..num_partitions)
                .map(|idx| {
                    Some(BatchState {
                        batch_idx: idx,
                        row_idx: 0,
                        exhausted: false,
                    })
                })
                .collect(),
        }
    }

    /// Create a new k-way merger using the stored state.
    ///
    /// The batches vec should hold Some for states that are still Some, and
    /// None for states that are None.
    pub fn create_merger<'a>(
        &mut self,
        batches: Vec<Option<&'a SortedBatch>>,
    ) -> Result<KWayMerger<'a, BatchStateIter<'a>>> {
        let mut iters = Vec::new();
        for (batch, state) in batches.into_iter().zip(self.states.iter_mut()) {
            match (batch, state) {
                (Some(sorted), Some(state)) => {
                    let iter = BatchStateIter { state, sorted };
                    iters.push(iter);
                }
                (None, None) => continue,
                _ => return Err(RayexecError::new("States out of sync")),
            }
        }

        Ok(KWayMerger::new(iters))
    }

    pub fn set_state_none(&mut self, idx: usize) {
        self.states[idx] = None;
    }

    pub fn find_exhausted(&self) -> Option<usize> {
        self.states.iter().find_map(|state| match state.as_ref() {
            Some(state) if state.exhausted => Some(state.batch_idx),
            _ => None,
        })
    }

    /// Reset a batch state.
    pub fn reset_state(&mut self, idx: usize) {
        match &mut self.states[idx] {
            Some(state) => {
                state.row_idx = 0;
                state.exhausted = false;
            }
            None => panic!("attempted to reset a state for a partition that's been exhausted"),
        }
    }
}

#[derive(Debug)]
struct BatchState {
    /// Read-only batch index.
    batch_idx: usize,

    /// Start row index that gets updated during iterating the sorted batch.
    row_idx: usize,

    /// Marker if this iterator was exhausted.
    exhausted: bool,
}

#[derive(Debug)]
struct BatchStateIter<'a> {
    state: &'a mut BatchState,
    sorted: &'a SortedBatch,
}

impl<'a> Iterator for BatchStateIter<'a> {
    type Item = RowReference<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.state.row_idx >= self.sorted.batch.num_rows() {
            self.state.exhausted = true;
            return None;
        }

        let row_idx = self.state.row_idx;
        let row = self.sorted.keys.row(row_idx).expect("row to exist");
        self.state.row_idx += 1;

        let reference = RowReference {
            batch_idx: self.state.batch_idx,
            row_idx,
            key: row,
        };

        Some(reference)
    }
}

/// Behavior of what happens when one of the iterators producing row references
/// is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KWayMergeExhaustBehavior {
    /// Continue with all the other iterators.
    Continue,

    /// Break and return early.
    Break,
}

#[derive(Debug)]
pub struct KWayMerger<'a, I: Iterator<Item = RowReference<'a>>> {
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
    row_iters: Vec<I>,

    /// Interleave indices buffer, (batch_idx, row_idx)
    ///
    /// Used to avoid reallocating everytime we generate new indices.
    indices_buf: Vec<(usize, usize)>,
}

impl<'a, I> KWayMerger<'a, I>
where
    I: Iterator<Item = RowReference<'a>>,
{
    pub fn new(mut row_iters: Vec<I>) -> Self {
        let mut heap = BinaryHeap::with_capacity(row_iters.len());

        // Fill up initial heap state.
        for iter in &mut row_iters {
            let reference = iter.next().expect("iters to not be empty");
            heap.push(reference);
        }

        KWayMerger {
            heap,
            row_iters,
            indices_buf: Vec::new(),
        }
    }

    /// Get the next set of interleave indices which would produce a totally
    /// ordered batch.
    pub fn interleave_indices(
        &mut self,
        exhaust_behavior: KWayMergeExhaustBehavior,
        max_batch_size: usize,
    ) -> Option<&[(usize, usize)]> {
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
            match self.row_iters[reference.batch_idx].next() {
                Some(next) => self.heap.push(next),
                None => match exhaust_behavior {
                    KWayMergeExhaustBehavior::Continue => (),
                    KWayMergeExhaustBehavior::Break => break,
                },
            }

            self.indices_buf
                .push((reference.batch_idx, reference.row_idx))
        }

        Some(&self.indices_buf)
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
