use std::task::Context;
use std::{fmt, task::Poll};

use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use rayexec_execution::{
    execution::operators::{PollFinalize, PollPush},
    functions::copy::{CopyToFunction, CopyToSink},
};
use rayexec_io::{FileLocation, FileSink};

use crate::writer::AsyncCsvWriter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CsvCopyToFunction;

impl CopyToFunction for CsvCopyToFunction {
    fn name(&self) -> &'static str {
        "csv_copy_to"
    }

    fn create_sinks(
        &self,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn CopyToSink>>> {
        unimplemented!()
    }
}

pub struct CsvCopyToSink {
    writer: AsyncCsvWriter<Box<dyn FileSink>>,
}

impl CopyToSink for CsvCopyToSink {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush> {
        // if let Some(mut future) = self.future.take() {
        //     match future.poll_unpin(cx) {
        //         Poll::Ready(Ok(_)) => (),
        //         Poll::Ready(Err(e)) => return Err(e),
        //         Poll::Pending => {
        //             self.future = Some(future);
        //             return Ok(PollPush::Pending(batch));
        //         }
        //     }
        // }

        unimplemented!()
    }

    fn poll_finalize(&mut self, cx: &mut Context) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl fmt::Debug for CsvCopyToSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvCopyToSink")
            .field("writer", &self.writer)
            .finish_non_exhaustive()
    }
}
