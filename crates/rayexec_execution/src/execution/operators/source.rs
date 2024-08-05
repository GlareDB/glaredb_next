use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use crate::logical::explainable::Explainable;
use std::fmt::Debug;

pub trait QuerySource: Debug + Send + Explainable {
    fn create_partition_sources(&self, num_sources: usize) -> Vec<Box<dyn PartitionSource>>;
    fn partition_requirement(&self) -> Option<usize>;
}

pub trait PartitionSource: Debug + Send {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>>;
}
