use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use rayexec_io::http::HttpClient;
use uuid::Uuid;

use crate::{
    execution::operators::{
        sink::{PartitionSink, QuerySink},
        source::{PartitionSource, QuerySource},
    },
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};

use super::client::{HybridClient, PullStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId {
    pub query_id: Uuid,
    pub stream_id: Uuid,
}

#[derive(Debug)]
pub struct ClientToServerStream<C: HttpClient> {
    stream_id: StreamId,
    client: Arc<HybridClient<C>>,
}

impl<C: HttpClient + 'static> QuerySink for ClientToServerStream<C> {
    fn create_partition_sinks(&self, num_sinks: usize) -> Vec<Box<dyn PartitionSink>> {
        assert_eq!(1, num_sinks);

        vec![Box::new(ClientToServerPartitionSink {
            stream_id: self.stream_id,
            client: self.client.clone(),
        })]
    }

    fn partition_requirement(&self) -> Option<usize> {
        Some(1)
    }
}

impl<C: HttpClient> Explainable for ClientToServerStream<C> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ClientToServerStream")
    }
}

#[derive(Debug)]
pub struct ClientToServerPartitionSink<C: HttpClient> {
    stream_id: StreamId,
    client: Arc<HybridClient<C>>,
}

impl<C: HttpClient> PartitionSink for ClientToServerPartitionSink<C> {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        // TODO: Figure out backpressure
        Box::pin(async { self.client.push(&self.stream_id, 0, batch).await })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { self.client.finalize(&self.stream_id, 0).await })
    }
}

#[derive(Debug)]
pub struct ServerToClientStream<C: HttpClient> {
    stream_id: StreamId,
    client: Arc<HybridClient<C>>,
}

impl<C: HttpClient + 'static> QuerySource for ServerToClientStream<C> {
    fn create_partition_sources(&self, num_sources: usize) -> Vec<Box<dyn PartitionSource>> {
        assert_eq!(1, num_sources);

        vec![Box::new(ServerToClientPartitionSource {
            stream_id: self.stream_id,
            client: self.client.clone(),
        })]
    }

    fn partition_requirement(&self) -> Option<usize> {
        Some(1)
    }
}

impl<C: HttpClient> Explainable for ServerToClientStream<C> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ServerToClientStream")
    }
}

#[derive(Debug)]
pub struct ServerToClientPartitionSource<C: HttpClient> {
    stream_id: StreamId,
    client: Arc<HybridClient<C>>,
}

impl<C: HttpClient> PartitionSource for ServerToClientPartitionSource<C> {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async {
            // TODO
            loop {
                let status = self.client.pull(&self.stream_id, 0).await?;
                match status {
                    PullStatus::Batch(batch) => return Ok(Some(batch)),
                    PullStatus::Pending => continue,
                    PullStatus::Finished => return Ok(None),
                }
            }
        })
    }
}
