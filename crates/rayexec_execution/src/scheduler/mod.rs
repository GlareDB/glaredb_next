pub mod future;
pub mod query;

use rayon::ThreadPool;
use std::fmt;
use std::sync::Arc;

use crate::execution::pipeline::PartitionPipeline;

use self::query::PartitionPipelineTask;

/// Scheduler for executing queries and other tasks.
pub struct Scheduler {
    pool: Arc<ThreadPool>,
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("num_threads", &self.pool.current_num_threads())
            .finish_non_exhaustive()
    }
}

impl Scheduler {
    /// Executes the pipeline on the thread pool.
    pub fn spawn_partition_pipeline(&self, pipeline: PartitionPipeline) {
        let task = PartitionPipelineTask::new(pipeline);
        task.execute(self.pool.clone());
    }
}
