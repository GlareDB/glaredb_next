use dyn_clone::DynClone;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use rayexec_io::FileLocation;
use std::{fmt::Debug, task::Context};

use crate::execution::operators::PollPush;

pub trait CopyToFunction: Debug + Sync + Send + DynClone {
    /// Name of the copy to function.
    fn name(&self) -> &'static str;

    /// Create a COPY TO destination that will write to the given location.
    // TODO: Additional COPY TO args once we have them.
    fn create_destination(&self, location: FileLocation) -> Result<Box<dyn CopyToDestination>>;
}

impl Clone for Box<dyn CopyToFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn CopyToFunction> for Box<dyn CopyToFunction + '_> {
    fn eq(&self, other: &dyn CopyToFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn CopyToFunction + '_ {
    fn eq(&self, other: &dyn CopyToFunction) -> bool {
        self.name() == other.name()
    }
}

pub trait CopyToDestination: Debug + Sync + Send {
    fn create_sinks(&self, num_partitions: usize) -> Result<Vec<Box<dyn CopyToSink>>>;
}

pub trait CopyToSink: Debug + Sync + Send {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush>;
    fn finalize(&mut self) -> Result<()>;
}
