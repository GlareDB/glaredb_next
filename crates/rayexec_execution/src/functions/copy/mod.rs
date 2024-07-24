use dyn_clone::DynClone;
use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_io::location::FileLocation;
use std::fmt::Debug;
use std::sync::Arc;

use crate::runtime::ExecutionRuntime;

pub trait CopyToFunction: Debug + Sync + Send + DynClone {
    /// Name of the copy to function.
    fn name(&self) -> &'static str;

    /// Create a COPY TO destination that will write to the given location.
    // TODO: Additional COPY TO args once we have them.
    fn create_sinks(
        &self,
        runtime: &Arc<dyn ExecutionRuntime>,
        schema: Schema,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn CopyToSink>>>;
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

pub trait CopyToSink: Debug + Send {
    /// Push a batch to the sink.
    ///
    /// Batches are pushed in the order they're received in.
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>>;

    /// Finalize the sink.
    ///
    /// Called once only after all batches have been pushed. If there's any
    /// pending work that needs to happen (flushing), it should happen here.
    /// Once this returns, the sink is complete.
    fn finalize(&mut self) -> BoxFuture<'_, Result<()>>;
}
