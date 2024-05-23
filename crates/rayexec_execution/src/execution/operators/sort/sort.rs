use crate::{
    execution::operators::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush},
    expr::PhysicalSortExpression,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

use super::util::sort_data::{PartitionTotalSortData, PartitionWorkingSortData};

#[derive(Debug)]
pub enum SortPartitionState {
    /// Partition is accepting data for sorting.
    Consuming {
        /// In-progress sorting data.
        sort_data: PartitionWorkingSortData,

        /// Waker on the pull side that tried to get a batch before we were done
        /// sorting this partition.
        pull_waker: Option<Waker>,
    },

    /// Partition is producing sorted data.
    Producing {
        /// Data that's been totally sorted for this partition.
        sort_data: PartitionTotalSortData,
    },
}

/// Physical operator for sorting batches within a partition.
#[derive(Debug)]
pub struct PhysicalSort {
    exprs: Vec<PhysicalSortExpression>,
}

impl PhysicalSort {
    pub fn create_states(&self, partitions: usize) -> Vec<SortPartitionState> {
        (0..partitions)
            .map(|_| SortPartitionState::Consuming {
                sort_data: PartitionWorkingSortData::new(&self.exprs),
                pull_waker: None,
            })
            .collect()
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
            SortPartitionState::Consuming { sort_data, .. } => {
                sort_data.push_batch(batch)?;
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
                sort_data,
                pull_waker,
            } => {
                let pull_waker = pull_waker.take(); // Taken here to satisfy lifetime.

                // Sort this partition's data and update state.
                // TODO: Configure batch size.
                let sort_data = sort_data.try_into_total_sort(1024)?;
                *state = SortPartitionState::Producing { sort_data };

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
            SortPartitionState::Producing { sort_data } => {
                match sort_data.next() {
                    Some(batch) => Ok(PollPull::Batch(batch)),
                    None => {
                        // This partition is done.
                        Ok(PollPull::Exhausted)
                    }
                }
            }
        }
    }
}
