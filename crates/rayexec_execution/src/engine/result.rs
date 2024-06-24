use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use rayexec_bullet::{batch::Batch, field::Schema};
use rayexec_error::Result;

use crate::scheduler::handle::QueryHandle;

use super::stream::QueryStream;

#[derive(Debug)]
pub struct ExecutionResult {
    pub output_schema: Schema,
    pub stream: QueryStream,
    pub handle: QueryHandle,
}

impl Stream for ExecutionResult {
    type Item = Result<Batch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(e) = self.handle.errors.1.try_recv() {
            return Poll::Ready(Some(Err(e)));
        }

        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
