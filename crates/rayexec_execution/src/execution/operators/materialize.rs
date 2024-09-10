use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::logical::binder::bind_context::MaterializationRef;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};

use super::sink::{PartitionSink, SinkOperation};
use super::source::{PartitionSource, QuerySource};
use super::util::broadcast::{BroadcastChannel, BroadcastReceiver};

#[derive(Debug)]
pub struct MaterializedOperator {
    pub sink: MaterializedSink,
    pub sources: Vec<MaterializedSource>,
}

impl MaterializedOperator {
    pub fn new(mat_ref: MaterializationRef, partitions: usize, source_scans: usize) -> Self {
        let mut sinks = Vec::new();
        let mut sources: Vec<_> = (0..source_scans).map(|_| Vec::new()).collect();

        for _partition in 0..partitions {
            let (ch, recvs) = BroadcastChannel::new(source_scans);

            sinks.push(MaterializedDataPartitionSink { sender: ch });

            for (idx, recv) in recvs.into_iter().enumerate() {
                sources[idx].push(MaterializedDataPartitionSource { recv })
            }
        }

        let sources = sources
            .into_iter()
            .map(|scans| MaterializedSource {
                mat_ref,
                sources: Mutex::new(scans),
            })
            .collect();

        let sink = MaterializedSink {
            mat_ref,
            sinks: Mutex::new(sinks),
        };

        MaterializedOperator { sink, sources }
    }
}

#[derive(Debug)]
pub struct MaterializedSink {
    mat_ref: MaterializationRef,
    sinks: Mutex<Vec<MaterializedDataPartitionSink>>,
}

impl SinkOperation for MaterializedSink {
    fn create_partition_sinks(
        &self,
        _context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        let mut sinks = self.sinks.lock();
        let sinks = std::mem::replace(sinks.as_mut(), Vec::new());

        if sinks.len() != num_sinks {
            return Err(RayexecError::new(format!(
                "Invalid sinks len: {}, expected: {}",
                sinks.len(),
                num_sinks,
            )));
        }

        Ok(sinks.into_iter().map(|s| Box::new(s) as _).collect())
    }

    fn partition_requirement(&self) -> Option<usize> {
        Some(self.sinks.lock().len())
    }
}

impl Explainable for MaterializedSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("MaterializedSink").with_value("materialized_ref", self.mat_ref)
    }
}

#[derive(Debug)]
pub struct MaterializedSource {
    mat_ref: MaterializationRef,
    sources: Mutex<Vec<MaterializedDataPartitionSource>>,
}

impl QuerySource for MaterializedSource {
    fn create_partition_sources(&self, num_sources: usize) -> Vec<Box<dyn PartitionSource>> {
        let mut sources = self.sources.lock();
        let sources = std::mem::replace(sources.as_mut(), Vec::new());

        if sources.len() != num_sources {
            panic!(
                "invalid sources len: {}, expected: {}",
                sources.len(),
                num_sources
            );
        }

        sources.into_iter().map(|s| Box::new(s) as _).collect()
    }

    fn partition_requirement(&self) -> Option<usize> {
        Some(self.sources.lock().len())
    }
}

impl Explainable for MaterializedSource {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("MaterializedSource").with_value("materialized_ref", self.mat_ref)
    }
}

#[derive(Debug)]
pub struct MaterializedDataPartitionSource {
    recv: BroadcastReceiver,
}

impl PartitionSource for MaterializedDataPartitionSource {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        let fut = self.recv.recv();
        Box::pin(async move { Ok(fut.await) })
    }
}

#[derive(Debug)]
pub struct MaterializedDataPartitionSink {
    sender: BroadcastChannel,
}

impl PartitionSink for MaterializedDataPartitionSink {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.sender.send(batch);
            Ok(())
        })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.sender.finish();
            Ok(())
        })
    }
}
