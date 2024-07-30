use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{channel::mpsc, future::BoxFuture, FutureExt, SinkExt, Stream, StreamExt};
use parking_lot::Mutex;
use rayexec_bullet::{batch::Batch, field::Schema};
use rayexec_error::{RayexecError, Result};
use tracing::warn;

use crate::{
    execution::operators::{
        sink::{PartitionSink, QuerySink},
        PollFinalize, PollPush,
    },
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
    runtime::{ErrorSink, QueryHandle},
};

/// Create sinks and streams for sending query output to a client.
pub fn new_results_sinks() -> (ResultStream, ResultSink, ResultErrorSink) {
    let (batch_tx, batch_rx) = mpsc::channel(1);
    let (err_tx, err_rx) = mpsc::channel(1);

    (
        ResultStream { batch_rx, err_rx },
        ResultSink { batch_tx },
        ResultErrorSink {
            err_tx: Mutex::new(err_tx),
        },
    )
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub output_schema: Schema,
    pub stream: ResultStream,
    pub handle: Box<dyn QueryHandle>,
}

#[derive(Debug)]
pub struct ResultStream {
    batch_rx: mpsc::Receiver<Batch>,
    err_rx: mpsc::Receiver<RayexecError>,
}

impl Stream for ResultStream {
    type Item = Result<Batch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(Some(err)) = self.err_rx.try_next() {
            return Poll::Ready(Some(Err(err)));
        }

        match self.batch_rx.poll_next_unpin(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct ResultSink {
    batch_tx: mpsc::Sender<Batch>,
}

impl QuerySink for ResultSink {
    fn create_partition_sinks(&self, num_sinks: usize) -> Vec<Box<dyn PartitionSink>> {
        (0..num_sinks)
            .map(|_| {
                Box::new(ResultPartitionSink {
                    batch_tx: self.batch_tx.clone(),
                }) as _
            })
            .collect()
    }

    fn partition_requirement(&self) -> Option<usize> {
        Some(1)
    }
}

impl Explainable for ResultSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ResultSink")
    }
}

#[derive(Debug)]
pub struct ResultPartitionSink {
    batch_tx: mpsc::Sender<Batch>,
}

impl PartitionSink for ResultPartitionSink {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let _ = self.batch_tx.send(batch).await;
            Ok(())
        })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.batch_tx.close_channel();
            Ok(())
        })
    }
}

#[derive(Debug)]
pub struct ResultErrorSink {
    err_tx: Mutex<mpsc::Sender<RayexecError>>,
}

impl ErrorSink for ResultErrorSink {
    fn push_error(&self, error: RayexecError) {
        warn!(%error, "query error");

        let mut err_tx = match self.err_tx.try_lock() {
            Some(tx) => tx,
            None => {
                // Someone else already sending an error.
                return;
            }
        };
        // Errors only in the case of receiver disconnected, or channel doesn't
        // have sufficient capacity.
        //
        // If receiver disconnect, then who cares.
        //
        // If channel lacks capicity, it means we've already sent an error, so
        // the client is already getting notified about something.
        let _ = err_tx.try_send(error);
    }
}
