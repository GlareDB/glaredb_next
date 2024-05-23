use crate::{
    execution::operators::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush},
    expr::PhysicalSortExpression,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

use super::util::{
    merger::{KWayMerger, MergeResult},
    sort_keys::SortKeysExtractor,
    sorted_batch::{IndexSortedBatch, SortedIndicesIter},
};

#[derive(Debug)]
pub enum SortPartitionState {
    /// Partition is accepting data for sorting.
    Consuming {
        /// Extract the sort keys from a batch.
        extractor: SortKeysExtractor,

        /// Batches that we sorted the row indices for.
        ///
        /// Batches are not sorted relative to each other.
        batches: Vec<IndexSortedBatch>,

        /// Waker on the pull side that tried to get a batch before we were done
        /// sorting this partition.
        pull_waker: Option<Waker>,
    },

    /// Partition is producing sorted data.
    Producing {
        /// Merger for merging all batches in this partition.
        merger: KWayMerger<SortedIndicesIter>,
    },
}

/// Physical operator for sorting batches within a partition.
#[derive(Debug)]
pub struct PhysicalSort {
    exprs: Vec<PhysicalSortExpression>,
}

impl PhysicalSort {
    pub fn create_states(&self, partitions: usize) -> Vec<SortPartitionState> {
        unimplemented!()
        // (0..partitions)
        //     .map(|_| SortPartitionState::Consuming {
        //         sort_data: PartitionWorkingSortData::new(&self.exprs),
        //         pull_waker: None,
        //     })
        //     .collect()
    }
}

impl PhysicalOperator for PhysicalSort {
    fn poll_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::Sort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            SortPartitionState::Consuming {
                extractor, batches, ..
            } => {
                let keys = extractor.sort_keys(&batch)?;

                // Produce the indices that would result in a sorted batches. We
                // can use these indices later to `interleave` rows once we want
                // to start returning sorted batches.
                let mut sort_indices: Vec<_> = (0..batch.num_rows()).collect();
                sort_indices.sort_by_key(|idx| keys.row(*idx).expect("row to exist"));

                let batch = IndexSortedBatch {
                    sort_indices,
                    keys,
                    batch,
                };
                batches.push(batch);

                Ok(PollPush::NeedsMore)
            }
            SortPartitionState::Producing { .. } => {
                panic!("attempted to push to partition that's already produding data")
            }
        }
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        let state = match partition_state {
            PartitionState::Sort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state {
            SortPartitionState::Consuming {
                batches,
                pull_waker,
                ..
            } => {
                let pull_waker = pull_waker.take(); // Taken here to satisfy lifetime.

                // Initialize the merger with all the batches.
                let mut merger = KWayMerger::new(batches.len());
                let batches = std::mem::take(batches);

                // Index is arbitrary here. Just used to identify batches within
                // the partition.
                for (idx, batch) in batches.into_iter().enumerate() {
                    let (batch, iter) = batch.into_batch_and_iter(idx);
                    merger.push_batch_for_input(idx, batch, iter);
                }

                // Wake up thread waiting to pull.
                if let Some(waker) = pull_waker {
                    waker.wake()
                }

                Ok(())
            }
            SortPartitionState::Producing { .. } => {
                panic!("attempted to finalize partition that's already producing data")
            }
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let mut state = match partition_state {
            PartitionState::Sort(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match &mut state {
            SortPartitionState::Consuming { pull_waker, .. } => {
                // Partition still collecting data to sort.
                *pull_waker = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
            SortPartitionState::Producing { merger } => {
                loop {
                    // TODO: Configurable batch size.
                    match merger.try_merge(1024)? {
                        MergeResult::Batch(batch) => return Ok(PollPull::Batch(batch)),
                        MergeResult::Exhausted => return Ok(PollPull::Exhausted),
                        MergeResult::NeedsInput(idx) => {
                            // We're merging all batch in this partition, and
                            // the merger already has everything, so we go ahead
                            // and mark this batch as complete.
                            merger.input_finished(idx);
                            // Continue to keep merging...
                        }
                    }
                }
            }
        }
    }
}
