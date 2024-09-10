use hashbrown::HashMap;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_io::http::HttpClient;
use std::{collections::VecDeque, sync::Arc};

use crate::{
    database::DatabaseContext,
    engine::result::ResultSink,
    execution::{
        intermediate::{
            IntermediateMaterializationGroup, IntermediateOperator, IntermediatePipeline,
            IntermediatePipelineGroup, IntermediatePipelineId, PipelineSink, PipelineSource,
        },
        operators::{
            materialize::MaterializedOperator,
            round_robin::{round_robin_states, PhysicalRoundRobinRepartition},
            sink::{SinkOperation, SinkOperator},
            source::PhysicalQuerySource,
            ExecutableOperator, InputOutputStates, OperatorState, PartitionState, PhysicalOperator,
        },
    },
    hybrid::{
        buffer::ServerStreamBuffers,
        client::HybridClient,
        stream::{ClientToServerStream, ServerToClientStream},
    },
    logical::binder::bind_context::MaterializationRef,
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
        ///
        /// Should only be used once per query. The option helps us enforce that
        /// (and also allows us to avoid needing to wrap in an Arc).
        output_sink: Option<ResultSink>,
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
    /// Location specific state used during planning.
    loc_state: PlanLocationState<'a, R::HttpClient>,
}

impl<'a, R: Runtime> ExecutablePipelinePlanner<'a, R> {
    pub fn new(
        context: &'a DatabaseContext,
        config: ExecutionConfig,
        loc_state: PlanLocationState<'a, R::HttpClient>,
    ) -> Self {
        ExecutablePipelinePlanner {
            context,
            config,
            id_gen: PipelineIdGen { gen: PipelineId(0) },
            loc_state,
        }
    }

    pub fn plan_from_intermediate(
        &mut self,
        group: IntermediatePipelineGroup,
        materializations: IntermediateMaterializationGroup,
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
                // Source is pipeline that's executing somewhere else.
                let operator = match &self.loc_state {
                    PlanLocationState::Server { stream_buffers } => {
                        let source = stream_buffers.create_incoming_stream(stream_id)?;
                        PhysicalQuerySource::new(Box::new(source))
                    }
                    PlanLocationState::Client { hybrid_client, .. } => {
                        // Missing hybrid client shouldn't happen.
                        let hybrid_client = hybrid_client.ok_or_else(|| {
                            RayexecError::new("Hybrid client missing, cannot create sink pipeline")
                        })?;
                        let source = ServerToClientStream::new(stream_id, hybrid_client.clone());
                        PhysicalQuerySource::new(Box::new(source))
                    }
                };

                let states = operator.create_states(self.context, vec![partitions])?;
                let partition_states = match states.partition_states {
                    InputOutputStates::OneToOne { partition_states } => partition_states,
                    _ => {
                        return Err(RayexecError::new(
                            "Invalid partition states for query source",
                        ))
                    }
                };

                let mut pipeline = ExecutablePipeline::new(self.id_gen.next(), partitions);
                pipeline.push_operator(
                    Arc::new(operator),
                    states.operator_state,
                    partition_states,
                )?;

                pipeline
            }
            PipelineSource::Materialization { mat_ref } => {
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
            PipelineSink::QueryOutput => {
                let sink = match &mut self.loc_state {
                    PlanLocationState::Client { output_sink, .. } => match output_sink.take() {
                        Some(sink) => sink,
                        None => return Err(RayexecError::new("Missing output sink")),
                    },
                    PlanLocationState::Server { .. } => {
                        return Err(RayexecError::new("Query output needs to happen on client"))
                    }
                };

                let partitions = match sink.partition_requirement() {
                    Some(n) => n,
                    None => pipeline.num_partitions(),
                };

                if partitions != pipeline.num_partitions() {
                    pipeline = self.push_repartition(pipeline, partitions, executables)?;
                }

                let operator = Arc::new(PhysicalOperator::ResultSink(SinkOperator::new(sink)));
                let states = operator.create_states(self.context, vec![partitions])?;
                let partition_states = match states.partition_states {
                    InputOutputStates::OneToOne { partition_states } => partition_states,
                    _ => return Err(RayexecError::new("invalid partition states for query sink")),
                };

                pipeline.push_operator(operator, states.operator_state, partition_states)?;
            }
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
                let operator: SinkOperator<Box<dyn SinkOperation>> = match &self.loc_state {
                    PlanLocationState::Server { stream_buffers } => {
                        let sink = stream_buffers.create_outgoing_stream(stream_id)?;
                        SinkOperator::new(Box::new(sink))
                    }
                    PlanLocationState::Client { hybrid_client, .. } => {
                        // Missing hybrid client shouldn't happen. Means we've
                        // incorrectly planned a hybrid query when we shouldn't
                        // have.
                        let hybrid_client = hybrid_client.ok_or_else(|| {
                            RayexecError::new("Hybrid client missing, cannot create sink pipeline")
                        })?;
                        let sink = ClientToServerStream::new(stream_id, hybrid_client.clone());
                        SinkOperator::new(Box::new(sink))
                    }
                };

                let states = operator.create_states(self.context, vec![partitions])?;
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
            PipelineSink::Materialization { mat_ref } => {
                unimplemented!()
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
            let pending = PendingOperatorWithState::try_from_intermediate_operator(
                &self.config,
                self.context,
                operator,
            )?;

            let idx = operators.len();
            operators.push(pending);
            pipeline.operators.push(idx);
        }

        Ok(pipeline)
    }
}

#[derive(Debug)]
struct PendingQuery {
    /// All pending operators in a query.
    operators: Vec<PendingOperatorWithState>,
    /// Pending pipelines in this query.
    ///
    /// This includes pipelines that make up a materialization.
    pipelines: HashMap<IntermediatePipelineId, PendingPipeline>,
    /// Pending materializations in the query.
    materializations: HashMap<MaterializationRef, PendingMaterialization>,
}

impl PendingQuery {
    fn try_from_operators_and_materializations(
        config: &ExecutionConfig,
        context: &DatabaseContext,
        group: IntermediatePipelineGroup,
        materializations: IntermediateMaterializationGroup,
    ) -> Result<Self> {
        let mut pending_materializations = HashMap::new();
        let mut operators = Vec::new();

        // Handle materializations first.
        for (mat_ref, materialization) in materializations.materializations {
            let mut operator_indexes = Vec::with_capacity(materialization.operators.len());

            for operator in materialization.operators {
                let idx = operators.len();
                let pending = PendingOperatorWithState::try_from_intermediate_operator(
                    config, context, operator,
                )?;

                operator_indexes.push(idx);
                operators.push(pending);
            }

            let mat_op = MaterializedOperator::new(
                mat_ref,
                config.target_partitions,
                materialization.scan_count,
            );

            // Add materialization sink to pending operators.
            let idx = operators.len();
            let pending = PendingOperatorWithState::try_from_intermediate_operator(
                config,
                context,
                IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::HybridSink(SinkOperator::new(Box::new(
                        mat_op.sink,
                    )))),
                    partitioning_requirement: Some(config.target_partitions),
                },
            )?;
            operators.push(pending);
            operator_indexes.push(idx);

            let scan_sources = mat_op
                .sources
                .into_iter()
                .map(|s| PhysicalQuerySource::new(Box::new(s)))
                .collect();

            pending_materializations.insert(
                mat_ref,
                PendingMaterialization {
                    operators: operator_indexes,
                    source: materialization.source,
                    scan_sources,
                },
            );
        }

        let mut pending_pipelines = HashMap::new();

        // Handle the other operators.
        for (id, pipeline) in group.pipelines {
            let mut operator_indexes = Vec::with_capacity(pipeline.operators.len());

            for operator in pipeline.operators {
                let idx = operators.len();
                let pending = PendingOperatorWithState::try_from_intermediate_operator(
                    config, context, operator,
                )?;

                operator_indexes.push(idx);
                operators.push(pending);
            }

            pending_pipelines.insert(
                id,
                PendingPipeline {
                    operators: operator_indexes,
                    sink: pipeline.sink,
                    source: pipeline.source,
                },
            );
        }

        Ok(PendingQuery {
            operators,
            pipelines: pending_pipelines,
            materializations: pending_materializations,
        })
    }

    fn plan_executable_pipelines(&mut self) -> Result<Vec<ExecutablePipeline>> {
        let mut pipelines = Vec::with_capacity(self.pipelines.len() + self.materializations.len());

        Ok(pipelines)
    }
}

#[derive(Debug)]
struct PendingMaterialization {
    /// Indices that index into the `operators` vec in pending query.
    ///
    /// The last index should point to the sink operator for the
    /// materialization.
    operators: Vec<usize>,
    /// Source for this pipeline.
    source: PipelineSource,
    /// Sources for operators that depend on this materialization.
    ///
    /// Length of the vector corresponds to the computed scan count for the
    /// materialization. An error should be returned if this is non-zero at the
    /// end of planning, or if there are more dependent pipelines than there are
    /// sources.
    scan_sources: Vec<PhysicalQuerySource>,
}

#[derive(Debug)]
struct PendingPipeline {
    /// Indices that index into the `operators` vec in pending query.
    operators: Vec<usize>,
    /// Sink for this pipeline.
    sink: PipelineSink,
    /// Source for this pipeline.
    source: PipelineSource,
}

/// An operator with initialized state.
#[derive(Debug)]
struct PendingOperatorWithState {
    /// The physical operator.
    operator: Arc<PhysicalOperator>,
    /// Global operator state.
    operator_state: Arc<OperatorState>,
    /// Input states that get taken when building up the final execution
    /// pipeline.
    input_states: Vec<Option<Vec<PartitionState>>>,
    /// Output states that get popped when building the final pipeline.
    ///
    /// May be empty if the operator uses the same partition state for pushing
    /// and pulling.
    pull_states: VecDeque<Vec<PartitionState>>,
    /// Index of the input state to use for the pull state. This corresponds to
    /// the "trunk" of the pipeline.
    trunk_idx: usize,
}

impl PendingOperatorWithState {
    fn try_from_intermediate_operator(
        config: &ExecutionConfig,
        context: &DatabaseContext,
        operator: IntermediateOperator,
    ) -> Result<Self> {
        let partitions = operator
            .partitioning_requirement
            .unwrap_or(config.target_partitions);

        // TODO: How to get other input partitions.
        let states = operator.operator.create_states(context, vec![partitions])?;

        Ok(match states.partition_states {
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
        })
    }
}
