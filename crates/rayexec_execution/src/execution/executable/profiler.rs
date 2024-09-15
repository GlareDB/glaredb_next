use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::Duration;

use super::pipeline::{ExecutablePartitionPipeline, PipelineId};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryProfileData {
    /// Profile data for all pipelines in this query.
    pub pipelines: BTreeMap<PipelineId, PipelineProfileData>,
}

impl QueryProfileData {
    pub fn add_partition_data(&mut self, partition: &ExecutablePartitionPipeline) {
        let pipeline_data = self
            .pipelines
            .entry(partition.pipeline_id())
            .or_insert(PipelineProfileData::default());

        let partition_data = PartitionPipelineProfileData {
            operators: partition
                .operators()
                .iter()
                .map(|op| op.profile_data().clone())
                .collect(),
        };

        pipeline_data
            .partitions
            .insert(partition.partition(), partition_data);
    }
}

impl fmt::Display for QueryProfileData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (id, pipeline) in &self.pipelines {
            writeln!(f, "Pipeline {id:?}")?;

            for (id, partition) in &pipeline.partitions {
                writeln!(f, "  Partition {id}")?;

                writeln!(
                    f,
                    "    [{:>2}]  {:>8}  {:>8}  {}",
                    "Op", "Read", "Emitted", "Elapsed (micro)",
                )?;

                for (idx, operator) in partition.operators.iter().enumerate() {
                    writeln!(
                        f,
                        "    [{:>2}]  {:>8}  {:>8}  {}",
                        idx,
                        operator.rows_read,
                        operator.rows_emitted,
                        operator.elapsed.as_micros()
                    )?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineProfileData {
    /// Profile data for all partitions in this pipeline.
    ///
    /// Keyed by the partition number within the pipeline.
    pub partitions: BTreeMap<usize, PartitionPipelineProfileData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPipelineProfileData {
    /// Profile data for all operators in this partition pipeline.
    pub operators: Vec<OperatorProfileData>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OperatorProfileData {
    /// Number of rows read into the operator.
    pub rows_read: usize,
    /// Number of rows produced by the operator.
    pub rows_emitted: usize,
    /// Elapsed time while activley executing this operator.
    pub elapsed: Duration,
}
