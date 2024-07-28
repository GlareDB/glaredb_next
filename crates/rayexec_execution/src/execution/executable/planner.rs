use hashbrown::HashMap;
use rayexec_error::{RayexecError, Result};
use std::{collections::VecDeque, sync::Arc};

use crate::{
    database::DatabaseContext,
    execution::{
        intermediate::{
            IntermediatePipeline, IntermediatePipelineGroup, PipelineSink, PipelineSource,
        },
        operators::{
            ipc::PhysicalIpcSource,
            round_robin::{round_robin_states, PhysicalRoundRobinRepartition},
            OperatorState, PartitionState, PhysicalOperator,
        },
        pipeline::{ExecutablePipeline, PipelineId},
    },
};

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
struct PendingOperator {
    partitions: usize,
    operator: Arc<dyn PhysicalOperator>,
    operator_state: Arc<OperatorState>,
    input_states: Vec<Option<Vec<PartitionState>>>,
}

#[derive(Debug)]
struct PendingPipeline {
    operators: VecDeque<PendingOperator>,
    sink: PipelineSink,
    source: PipelineSource,
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
        let mut pending_pipelines = HashMap::with_capacity(group.pipelines.len());

        // Initial states.
        for (id, intermediate) in group.pipelines {
            let pending = self.plan_pending_pipeline(intermediate)?;
            pending_pipelines.insert(id, pending);
        }

        let pipelines = Vec::with_capacity(pending_pipelines.len());

        // Wire everything up.
        for pending in pending_pipelines.values_mut() {
            // Create initial pipeline from the source operator.
            let mut pipeline = match pending.source {
                PipelineSource::InPipeline => {
                    // Source is the first operator.
                    let mut source = pending.operators.pop_front().unwrap();
                    debug_assert_eq!(1, source.input_states.len());
                    let partition_states = source.input_states[0].take().unwrap();

                    let mut pipeline =
                        ExecutablePipeline::new(self.id_gen.next(), partition_states.len());
                    pipeline.push_operator(
                        source.operator,
                        source.operator_state,
                        partition_states,
                    )?;

                    pipeline
                }
                PipelineSource::Remote { partitions } => {
                    // Need to insert a remote ipc source.
                    let operator = Arc::new(PhysicalIpcSource {});
                    let mut states = operator.create_states(self.context, vec![partitions])?;
                    debug_assert_eq!(1, states.partition_states.len());

                    let partition_states = states.partition_states.pop().unwrap();

                    let mut pipeline =
                        ExecutablePipeline::new(self.id_gen.next(), partition_states.len());
                    pipeline.push_operator(operator, states.operator_state, partition_states)?;

                    pipeline
                }
            };

            // Wire up the rest.
            for pending_op in &mut pending.operators {
                debug_assert_eq!(1, pending_op.input_states.len());
                let partition_states = pending_op.input_states[0].take().unwrap();

                // If partition doesn't match, push a round robin and start new
                // pipeline.
                if partition_states.len() != pipeline.num_partitions() {
                    let rr_operator = Arc::new(PhysicalRoundRobinRepartition);
                    let (rr_state, push_states, pull_states) =
                        round_robin_states(pipeline.num_partitions(), partition_states.len());
                    let rr_state = Arc::new(OperatorState::RoundRobin(rr_state));

                    pipeline.push_operator(
                        rr_operator.clone(),
                        rr_state.clone(),
                        push_states
                            .into_iter()
                            .map(PartitionState::RoundRobinPush)
                            .collect(),
                    )?;

                    // New pipeline with round robin as source.
                    pipeline = ExecutablePipeline::new(self.id_gen.next(), pull_states.len());
                    pipeline.push_operator(
                        rr_operator,
                        rr_state,
                        pull_states
                            .into_iter()
                            .map(PartitionState::RoundRobinPull)
                            .collect(),
                    )?;

                    // Continue on, new pipeline now supports proper number of
                    // partitions.
                }

                pipeline.push_operator(
                    pending_op.operator.clone(),
                    pending_op.operator_state.clone(),
                    partition_states,
                )?;
            }

            // // Set up sink.
            // match pending.sink {
            //     PipelineSink::InGroup {
            //         pipeline,
            //         operator_idx,
            //         input_idx,
            //     } => {
            //         let sink_op =
            //             &mut pending_pipelines.get_mut(&pipeline).unwrap().operators[operator_idx];
            //         let operator = sink_op.operator.clone();
            //         let operator_state = sink_op.operator_state.clone();
            //         let partition_states = sink_op.input_states[input_idx].take().unwrap();

            //         // TODO: Round robin
            //     }
            //     PipelineSink::Remote { partitions } => {
            //         //
            //         unimplemented!()
            //     }
            // }
        }

        Ok(pipelines)
    }

    /// Plan a pending pipeline from an intermediate pipeline.
    ///
    /// This will create the states for all operators in the pipeline. The
    /// number of operators will not change.
    fn plan_pending_pipeline(&self, intermediate: IntermediatePipeline) -> Result<PendingPipeline> {
        let mut pipeline = PendingPipeline {
            operators: VecDeque::with_capacity(intermediate.operators.len() + 1),
            sink: intermediate.sink,
            source: intermediate.source,
        };

        // Create states for the rest.
        for operator in intermediate.operators {
            let partitions = operator
                .partitioning_requirement
                .unwrap_or(self.config.target_partitions);

            // TODO: How to get other input partitions.
            let states = operator
                .operator
                .create_states(self.context, vec![partitions])?;

            let input_states = states
                .partition_states
                .into_iter()
                .map(|states| Some(states))
                .collect();

            let pending = PendingOperator {
                partitions,
                operator: operator.operator,
                operator_state: states.operator_state,
                input_states,
            };

            pipeline.operators.push_back(pending);
        }

        Ok(pipeline)
    }
}
