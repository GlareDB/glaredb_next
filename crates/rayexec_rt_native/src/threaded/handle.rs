use std::{collections::BTreeMap, sync::Arc};

use parking_lot::Mutex;
use rayexec_execution::runtime::{
    dump::{PartitionPipelineDump, PipelineDump, QueryDump},
    QueryHandle,
};

use super::task::{PartitionPipelineTask, TaskState};

/// Query handle for queries being executed on the threaded runtime.
#[derive(Debug)]
pub struct ThreadedQueryHandle {
    /// Registered task states for all pipelines in a query.
    pub(crate) states: Mutex<Vec<Arc<TaskState>>>,
}

impl QueryHandle for ThreadedQueryHandle {
    /// Cancel the query.
    fn cancel(&self) {
        let states = self.states.lock();

        for state in states.iter() {
            let mut pipeline = state.pipeline.lock();
            pipeline.query_canceled = true;
            std::mem::drop(pipeline);

            // Re-execute the pipeline so it picks up the set bool. This lets us
            // cancel the pipeline regardless of if it's pending.
            let task = PartitionPipelineTask::from_task_state(state.clone());
            task.execute()
        }
    }

    fn dump(&self) -> QueryDump {
        use std::collections::btree_map::Entry;

        let mut dump = QueryDump {
            pipelines: BTreeMap::new(),
        };

        dump
    }
}
