use crate::execution::metrics::PartitionPipelineMetrics;
use parking_lot::Mutex;
use std::sync::mpsc;
use std::sync::Arc;

use super::query::{PartitionPipelineTask, TaskState};

/// A handle for all pipelines in a query.
#[derive(Debug)]
pub struct QueryHandle {
    /// Registered task states for all pipelines in a query.
    pub(crate) states: Mutex<Vec<Arc<TaskState>>>,

    /// Channel for sending metrics once a partition pipeline completes.
    pub(crate) metrics: (
        mpsc::Sender<PartitionPipelineMetrics>,
        mpsc::Receiver<PartitionPipelineMetrics>,
    ),
}

impl QueryHandle {
    /// Cancel the query.
    pub fn cancel(&self) {
        let mut states = self.states.lock();
        let states: Vec<_> = std::mem::take(states.as_mut());

        for state in states.into_iter() {
            let mut pipeline = state.pipeline.lock();
            pipeline.1 = true;
            std::mem::drop(pipeline);

            // Re-execute the pipeline so it picks up the set bool. This lets us
            // cancel the pipeline regardless of if it's pending.
            let task = PartitionPipelineTask::from_task_state(state);
            task.execute()
        }
    }
}
