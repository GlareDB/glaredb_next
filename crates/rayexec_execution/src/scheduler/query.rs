use crate::execution::{metrics::PartitionPipelineMetrics, pipeline::PartitionPipeline};
use parking_lot::Mutex;
use rayexec_error::RayexecError;
use rayon::ThreadPool;
use std::{
    sync::{
        mpsc::{self, SendError},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};
use tracing::debug;

/// State shared by the partition pipeline task and the waker.
#[derive(Debug)]
pub(crate) struct TaskState {
    /// The partition pipeline we're operating on alongside a boolean for if the
    /// query's been canceled.
    pub(crate) pipeline: Mutex<(PartitionPipeline, bool)>,

    /// Channel for sending errors that happen during execution.
    ///
    /// This isn't a oneshot since the same channel is shared across many
    /// partition pipelines that make up a query, and we want the option to
    /// collect them all, even if only first is shown to the user.
    pub(crate) errors: mpsc::Sender<RayexecError>,

    /// Optional channel for sending pipeline metrics once they complete.
    pub(crate) metrics: mpsc::Sender<PartitionPipelineMetrics>,

    /// The threadpool to execute on.
    pub(crate) pool: Arc<ThreadPool>,
}

/// Task for executing a partition pipeline.
pub struct PartitionPipelineTask {
    state: Arc<TaskState>,
}

impl PartitionPipelineTask {
    pub(crate) fn from_task_state(state: Arc<TaskState>) -> Self {
        PartitionPipelineTask { state }
    }

    pub(crate) fn execute(self) {
        let mut pipeline = self.state.pipeline.lock();

        if pipeline.1 {
            // Don't care about the error.
            let _ = self.state.errors.send(RayexecError::new("query canceled"));
            return;
        }

        let waker: Waker = Arc::new(PartitionPipelineWaker {
            state: self.state.clone(),
        })
        .into();

        let mut cx = Context::from_waker(&waker);
        loop {
            match pipeline.0.poll_execute(&mut cx) {
                Poll::Ready(Some(Ok(()))) => {
                    // Pushing through the pipeline was successful. Continue the
                    // loop to try to get as much work done as possible.
                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    if let Err(SendError(e)) = self.state.errors.send(e) {
                        debug!(%e, "errors receiver disconnected");
                    }
                }
                Poll::Pending => {
                    // Exit the loop. Waker was already stored in the pending
                    // sink/source, we'll be woken back up when there's more
                    // this operator chain can start executing.
                    return;
                }
                Poll::Ready(None) => {
                    // Partition pipeline finished. If we have a metrics
                    // channel, collect the metrics from the pipeline and send
                    // them out.
                    let metrics = pipeline.0.take_metrics();
                    let _ = self.state.metrics.send(metrics); // We don't care if the other side has been dropped.

                    // Exit the loop, nothing else for us to do. Waker is not
                    // stored, and we will not executed again.
                    return;
                }
            }
        }
    }
}

/// A waker implementation that will re-execute the pipeline once woken.
struct PartitionPipelineWaker {
    state: Arc<TaskState>,
}

impl Wake for PartitionPipelineWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let pool = self.state.pool.clone();
        let task = PartitionPipelineTask {
            state: self.state.clone(),
        };
        pool.spawn(|| task.execute());
    }
}
