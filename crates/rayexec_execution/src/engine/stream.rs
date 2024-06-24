use crate::execution::operators::PollPush;
use crate::execution::query_graph::sink::PartitionSink;
use futures::channel::mpsc;
use futures::{Stream, StreamExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct QueryStream {
    batches: (mpsc::Sender<Batch>, mpsc::Receiver<Batch>),
}

impl QueryStream {
    pub fn new() -> Self {
        QueryStream {
            batches: mpsc::channel(1),
        }
    }

    pub fn sink(&self) -> UnpartitionedSink {
        UnpartitionedSink {
            sender: self.batches.0.clone(),
        }
    }
}

impl Stream for QueryStream {
    type Item = Batch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.batches.1.poll_next_unpin(cx)
    }
}

#[derive(Debug)]
pub struct UnpartitionedSink {
    sender: mpsc::Sender<Batch>,
}

impl PartitionSink for UnpartitionedSink {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Ok(_)) => {
                match self.sender.start_send(batch) {
                    Ok(_) => Ok(PollPush::Pushed),
                    Err(_) => Ok(PollPush::Break), // TODO: What to do, receiving end disconnected between poll ready and start send.
                }
            }
            Poll::Ready(Err(e)) => {
                if e.is_full() {
                    Ok(PollPush::Pending(batch))
                } else {
                    // TODO: What to do? Receiving end closed.
                    Ok(PollPush::Break)
                }
            }
            Poll::Pending => Ok(PollPush::Pending(batch)),
        }
    }

    fn finalize_push(&mut self) -> Result<()> {
        self.sender.close_channel();
        Ok(())
    }
}
