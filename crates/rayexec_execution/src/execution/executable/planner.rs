use crate::{
    database::DatabaseContext,
    execution::{
        intermediate::IntermediatePipelineGroup,
        operators::{OperatorState, PartitionState, PhysicalOperator},
        pipeline::{ExecutablePipeline, PipelineId},
    },
};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

/// Mode of execution for the query.
#[derive(Debug, Clone, Copy)]
pub enum ExecutionMode {
    /// Part of the query is running here, part of it elsewhere.
    ///
    /// When encountering a location requirement change, insert the appropriate
    /// operator for pushing/pulling batches to/from the new location.
    // TODO: Include "this" location so we know where we are. Also needs to
    // include info for the "other" location. Include hybrid client idk.
    Hybrid,
    /// Query is being executed entirely on a single node (this node).
    ///
    /// If a location change is encountered, this will return an error.
    Single,
}

/// Used for ensuring every pipeline in a query has a unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PipelineIdGen {
    gen: PipelineId,
}

impl PipelineIdGen {
    fn next(&mut self) -> PipelineId {
        let id = self.gen;
        self.gen.0 += 1;
        id
    }
}

#[derive(Debug)]
struct PendingPipeline {
    pipeline: ExecutablePipeline,
    source_operator: Arc<dyn PhysicalOperator>,
    source_operator_state: Arc<OperatorState>,
    pending_partition_states: Option<Vec<PartitionState>>,
}

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub target_partitions: usize,
}

pub struct ExecutablePipelinePlanner<'a> {
    context: &'a DatabaseContext,
    config: ExecutionConfig,
    id_gen: PipelineIdGen,
}

impl<'a> ExecutablePipelinePlanner<'a> {
    pub fn plan_from_intermediate(
        &mut self,
        group: IntermediatePipelineGroup,
    ) -> Result<Vec<ExecutablePipeline>> {
        let mut pipelines = Vec::with_capacity(group.pipelines.len());

        for (_id, intermediate) in &group.pipelines {
            // Determine partitioning of this pipeline by seeing if the "source"
            // operator has a partitioning requirement.
            let partitions = match intermediate.operators.first() {
                Some(operator) => operator
                    .partitioning_requirement
                    .unwrap_or(self.config.target_partitions),
                None => {
                    return Err(RayexecError::new(
                        "Missing source operator in intermedate pipeline",
                    ))
                }
            };

            let mut pipeline = ExecutablePipeline::new(self.id_gen.next(), partitions);

            for operator in &intermediate.operators {
                // Check if this operator has a partitioning requirement. If
                // does and it doesn't match the partitioning of the current
                // pipeline, create a new pipeline with a round-robin linking
                // them.
                match operator.partitioning_requirement {
                    Some(req) if req != pipeline.num_partitions() => {
                        // TODO: Round robin here.

                        pipelines.push(pipeline);
                        pipeline = ExecutablePipeline::new(self.id_gen.next(), req)
                    }
                    _ => (),
                }

                // TODO: Create the states, push to pipeline.
                let _ = operator.operator.create_states()?;
            }
        }

        Ok(pipelines)
    }
}
