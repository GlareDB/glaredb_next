use hashbrown::HashMap;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_io::http::HttpClient;
use std::{collections::VecDeque, sync::Arc};

use crate::{
    database::DatabaseContext,
    execution::{
        intermediate::{
            IntermediatePipeline, IntermediatePipelineGroup, IntermediatePipelineId, PipelineSink,
            PipelineSource,
        },
        operators::{
            round_robin::{round_robin_states, PhysicalRoundRobinRepartition},
            sink::{PhysicalQuerySink, QuerySink},
            ExecutableOperator, InputOutputStates, OperatorState, PartitionState, PhysicalOperator,
        },
    },
    hybrid::{buffer::ServerStreamBuffers, client::HybridClient, stream::ClientToServerStream},
    runtime::Runtime,
};

use super::pipeline::{ExecutablePipeline, PipelineId};

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
struct PendingOperatorWithState {
    operator: Arc<PhysicalOperator>,
    operator_state: Arc<OperatorState>,
    input_states: Vec<Option<Vec<PartitionState>>>,
    pull_states: VecDeque<Vec<PartitionState>>,
    trunk_idx: usize,
}

#[derive(Debug)]
struct PendingPipeline {
    /// Indices into a `pending_operators` vec containing the operators and
    /// state.
    operators: Vec<usize>,
    sink: PipelineSink,
    source: PipelineSource,
}

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Target number of partitions in executable pipelines.
    ///
    /// Partitionining determines parallelism for a single pipeline.
    pub target_partitions: usize,
}

#[derive(Debug)]
pub enum PlanLocationState<'a, C: HttpClient> {
    /// State when planning on the server.
    Server {
        /// Stream buffers used for buffering incoming and outgoing batches for
        /// distributed execution.
        stream_buffers: &'a ServerStreamBuffers,
    },
    /// State when planning on the client side.
    Client {
        /// Output sink for a query.
        output_sink: Arc<dyn QuerySink>,
        /// Optional hybrid client if we're executing in hybrid mode.
        ///
        /// When providing, appropriate query sinks and sources will be inserted
        /// to the plan which will work to move batches between the client an
        /// server.
        hybrid_client: Option<&'a Arc<HybridClient<C>>>,
    },
}

#[derive(Debug)]
pub struct ExecutablePipelinePlanner<'a, R: Runtime> {
    context: &'a DatabaseContext,
    config: ExecutionConfig,
    id_gen: PipelineIdGen,
    /// Optional output sink.
    ///
    /// Should only be Some if the planner is planning a pipeline group with a
    /// pipeline that sends its results to the output.
    ///
    /// Only single pipeline may use this.
    output_sink: Option<Box<dyn QuerySink>>,

    /// Location specific state used during planning.
    loc_state: PlanLocationState<'a, R::HttpClient>,
}

impl<'a, R: Runtime> ExecutablePipelinePlanner<'a, R> {
    pub fn new(
        context: &'a DatabaseContext,
        config: ExecutionConfig,
        output_sink: Option<Box<dyn QuerySink>>,
    ) -> Self {
        unimplemented!()
        // ExecutablePipelinePlanner {
        //     context,
        //     config,
        //     id_gen: PipelineIdGen { gen: PipelineId(0) },
        //     output_sink,
        // }
    }

    pub fn plan_from_intermediate(
        &mut self,
        group: IntermediatePipelineGroup,
    ) -> Result<Vec<ExecutablePipeline>> {
        let mut pending_operators: Vec<PendingOperatorWithState> = Vec::new();
        let mut pending_pipelines: HashMap<_, PendingPipeline> =
            HashMap::with_capacity(group.pipelines.len());

        // Initial states.
        for (id, intermediate) in group.pipelines {
            let pipeline = self.plan_operators_with_state(&mut pending_operators, intermediate)?;
            pending_pipelines.insert(id, pipeline);
        }

        let mut pipelines = Vec::with_capacity(pending_pipelines.len());

        for pending in pending_pipelines.values() {
            self.plan_pending_pipeline(
                pending,
                &mut pending_operators,
                &pending_pipelines,
                &mut pipelines,
            )?;
        }

        Ok(pipelines)
    }

    fn plan_pending_pipeline(
        &mut self,
        pending: &PendingPipeline,
        operators: &mut [PendingOperatorWithState],
        pipelines: &HashMap<IntermediatePipelineId, PendingPipeline>,
        executables: &mut Vec<ExecutablePipeline>,
    ) -> Result<()> {
        let mut operator_indices = pending.operators.iter();

        // Create initial pipeline from the source operator.
        let mut pipeline = match pending.source {
            PipelineSource::InPipeline => {
                // Source is the first operator.
                let idx = operator_indices.next().unwrap();
                let source = &mut operators[*idx];
                debug_assert_eq!(1, source.input_states.len());
                let partition_states = source.input_states[0].take().unwrap();

                let mut pipeline =
                    ExecutablePipeline::new(self.id_gen.next(), partition_states.len());
                pipeline.push_operator(
                    source.operator.clone(),
                    source.operator_state.clone(),
                    partition_states,
                )?;

                pipeline
            }
            PipelineSource::OtherPipeline { pipeline } => {
                let operator_idx = pipelines
                    .get(&pipeline)
                    .required("pipeline")?
                    .operators
                    .last()
                    .required("at least one operator")?;
                let source = &mut operators[*operator_idx];

                // TODO: Definitely needs comments.
                let pull_states = source
                    .pull_states
                    .pop_front()
                    .required("separate pull states")?;

                let mut pipeline = ExecutablePipeline::new(self.id_gen.next(), pull_states.len());
                pipeline.push_operator(
                    source.operator.clone(),
                    source.operator_state.clone(),
                    pull_states,
                )?;

                pipeline
            }
            PipelineSource::OtherGroup {
                stream_id,
                partitions,
            } => {
                // Need to insert a remote ipc source.
                unimplemented!()
            }
        };

        // Wire up the rest.
        for operator_idx in operator_indices {
            let operator = &mut operators[*operator_idx];
            let partition_states = operator.input_states[operator.trunk_idx].take().unwrap();

            // If partition doesn't match, push a round robin and start new
            // pipeline.
            if partition_states.len() != pipeline.num_partitions() {
                pipeline = self.push_repartition(pipeline, partition_states.len(), executables)?;
            }

            pipeline.push_operator(
                operator.operator.clone(),
                operator.operator_state.clone(),
                partition_states,
            )?;
        }

        // Wire up sink.
        match pending.sink {
            PipelineSink::QueryOutput => match self.output_sink.take() {
                Some(sink) => {
                    let partitions = match sink.partition_requirement() {
                        Some(n) => n,
                        None => pipeline.num_partitions(),
                    };

                    if partitions != pipeline.num_partitions() {
                        pipeline = self.push_repartition(pipeline, partitions, executables)?;
                    }

                    let operator =
                        Arc::new(PhysicalOperator::QuerySink(PhysicalQuerySink::new(sink)));
                    let states = operator.create_states(&self.context, vec![partitions])?;
                    let partition_states = match states.partition_states {
                        InputOutputStates::OneToOne { partition_states } => partition_states,
                        _ => {
                            return Err(RayexecError::new(
                                "invalid partition states for query sink",
                            ))
                        }
                    };

                    pipeline.push_operator(operator, states.operator_state, partition_states)?;
                }
                None => {
                    return Err(RayexecError::new(
                        "Pipeline expects to send results to output, missing output sink",
                    ))
                }
            },
            PipelineSink::InPipeline => {
                // The pipeline's final operator is the query sink. A requisite
                // states have already been created from above, so nothing for
                // us to do.
            }
            PipelineSink::InGroup {
                pipeline_id,
                operator_idx,
                input_idx,
            } => {
                // We have the sink pipeline with us, wire up directly.

                let pending = pipelines.get(&pipeline_id).unwrap();
                let operator = &mut operators[pending.operators[operator_idx]];
                let partition_states = operator.input_states[input_idx].take().unwrap();

                if partition_states.len() != pipeline.num_partitions() {
                    pipeline =
                        self.push_repartition(pipeline, partition_states.len(), executables)?;
                }

                pipeline.push_operator(
                    operator.operator.clone(),
                    operator.operator_state.clone(),
                    partition_states,
                )?;
            }
            PipelineSink::OtherGroup {
                partitions,
                stream_id,
            } => {
                // Sink is pipeline executing somewhere else.
                let operator = match &self.loc_state {
                    PlanLocationState::Server { stream_buffers } => {
                        let sink = stream_buffers.create_outgoing_stream(stream_id);
                        PhysicalQuerySink::new(Box::new(sink))
                    }
                    PlanLocationState::Client { hybrid_client, .. } => {
                        // Missing hybrid client shouldn't happen. Means we've
                        // incorrectly planned a hybrid query when we shouldn't
                        // have.
                        let hybrid_client = hybrid_client.ok_or_else(|| {
                            RayexecError::new("Hybrid client missing, cannot create sink pipeline")
                        })?;
                        let sink = ClientToServerStream::new(stream_id, hybrid_client.clone());
                        PhysicalQuerySink::new(Box::new(sink))
                    }
                };

                let states = operator.create_states(&self.context, vec![partitions])?;
                let partition_states = match states.partition_states {
                    InputOutputStates::OneToOne { partition_states } => partition_states,
                    _ => return Err(RayexecError::new("invalid partition states")),
                };

                if partition_states.len() != pipeline.num_partitions() {
                    pipeline =
                        self.push_repartition(pipeline, partition_states.len(), executables)?;
                }

                pipeline.push_operator(
                    Arc::new(operator),
                    states.operator_state,
                    partition_states,
                )?;
            }
        }

        // And we're done, pipeline is complete.
        executables.push(pipeline);

        Ok(())
    }

    /// Push a repartition operator on the pipline, and return a new pipeline
    /// with the repartition as the source.
    fn push_repartition(
        &mut self,
        mut pipeline: ExecutablePipeline,
        output_partitions: usize,
        pipelines: &mut Vec<ExecutablePipeline>,
    ) -> Result<ExecutablePipeline> {
        let rr_operator = Arc::new(PhysicalRoundRobinRepartition);
        let (rr_state, push_states, pull_states) =
            round_robin_states(pipeline.num_partitions(), output_partitions);
        let rr_state = Arc::new(OperatorState::RoundRobin(rr_state));

        pipeline.push_operator(
            rr_operator.clone(),
            rr_state.clone(),
            push_states
                .into_iter()
                .map(PartitionState::RoundRobinPush)
                .collect(),
        )?;

        pipelines.push(pipeline);

        // New pipeline with round robin as source.
        let mut pipeline = ExecutablePipeline::new(self.id_gen.next(), pull_states.len());
        pipeline.push_operator(
            rr_operator,
            rr_state,
            pull_states
                .into_iter()
                .map(PartitionState::RoundRobinPull)
                .collect(),
        )?;

        Ok(pipeline)
    }

    /// Create pending operators with state and push to `operators`.
    ///
    /// The returned pipeline will have indices that reference the operators in
    /// `operators`.
    ///
    /// The number of operators produced matches the number of operators in the
    /// intermediate pipeline.
    fn plan_operators_with_state(
        &self,
        operators: &mut Vec<PendingOperatorWithState>,
        intermediate: IntermediatePipeline,
    ) -> Result<PendingPipeline> {
        let mut pipeline = PendingPipeline {
            operators: Vec::with_capacity(intermediate.operators.len()),
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

            let pending = match states.partition_states {
                InputOutputStates::OneToOne { partition_states } => PendingOperatorWithState {
                    operator: operator.operator,
                    operator_state: states.operator_state,
                    input_states: vec![Some(partition_states)],
                    pull_states: VecDeque::new(),
                    trunk_idx: 0,
                },
                InputOutputStates::NaryInputSingleOutput {
                    partition_states,
                    pull_states,
                } => {
                    let input_states: Vec<_> = partition_states.into_iter().map(Some).collect();
                    PendingOperatorWithState {
                        operator: operator.operator,
                        operator_state: states.operator_state,
                        input_states,
                        pull_states: VecDeque::new(),
                        trunk_idx: pull_states,
                    }
                }
                InputOutputStates::SingleInputNaryOutput {
                    push_states,
                    pull_states,
                } => PendingOperatorWithState {
                    operator: operator.operator,
                    operator_state: states.operator_state,
                    input_states: vec![Some(push_states)],
                    pull_states: pull_states.into_iter().collect(),
                    trunk_idx: 0,
                },
                InputOutputStates::SeparateInputOutput {
                    push_states,
                    pull_states,
                } => PendingOperatorWithState {
                    operator: operator.operator,
                    operator_state: states.operator_state,
                    input_states: vec![Some(push_states)],
                    pull_states: [pull_states].into_iter().collect(),
                    trunk_idx: 0,
                },
            };

            let idx = operators.len();
            operators.push(pending);
            pipeline.operators.push(idx);
        }

        Ok(pipeline)
    }
}
