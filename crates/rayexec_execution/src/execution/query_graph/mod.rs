pub mod explain;
pub mod planner;

use super::pipeline::{ExecutablePartitionPipeline, ExecutablePipeline};

#[derive(Debug)]
pub struct QueryGraph {
    /// All pipelines that make up this query.
    pipelines: Vec<ExecutablePipeline>,
}

impl QueryGraph {
    pub fn into_partition_pipeline_iter(self) -> impl Iterator<Item = ExecutablePartitionPipeline> {
        self.pipelines
            .into_iter()
            .flat_map(|pipeline| pipeline.into_partition_pipeline_iter())
    }
}
