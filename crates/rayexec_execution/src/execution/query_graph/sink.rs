use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::fmt::Debug;
use std::task::Context;

use crate::execution::operators::{PollFinalize, PollPush};

/// Where query results should be written.
#[derive(Debug)]
pub struct QuerySink {
    /// Per-partition sink.
    ///
    /// For client-facing interaction, this would typically be a vec of length 1
    /// where the partition sink just writes the batches to the client.
    ///
    /// However there might be cases where we could handle the partitions
    /// independently, like writing the output to disk where each partition is
    /// able to write to its own file.
    pub(crate) partition_sinks: Vec<Box<dyn PartitionSink>>,
}

impl QuerySink {
    /// Create a new query sink with the given partition sinks.
    pub fn new(sinks: impl IntoIterator<Item = Box<dyn PartitionSink>>) -> Self {
        QuerySink {
            partition_sinks: sinks.into_iter().collect(),
        }
    }

    /// Number of partitions that this sink is expected inputs to.
    ///
    /// Used during planning. If this differs from the number of partitions in
    /// the query result, the planner will repartition as appropriate to match
    /// this number.
    pub fn num_partitions(&self) -> usize {
        self.partition_sinks.len()
    }
}

/// How results for a partition should be written.
pub trait PartitionSink: Sync + Send + Debug {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>>;
    fn finalize(&mut self) -> BoxFuture<'_, Result<()>>;
}
