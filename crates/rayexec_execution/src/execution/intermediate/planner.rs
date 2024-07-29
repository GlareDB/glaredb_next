use crate::{
    execution::{
        intermediate::PipelineSink,
        operators::{
            copy_to::PhysicalCopyTo, empty::PhysicalEmpty, filter::FilterOperation,
            hash_aggregate::PhysicalHashAggregate, project::ProjectOperation,
            query_sink::PhysicalQuerySink, simple::SimpleOperator, union::PhysicalUnion,
            values::PhysicalValues,
        },
        pipeline::PipelineId,
        query_graph::sink::QuerySink,
    },
    expr::PhysicalScalarExpression,
    logical::{
        grouping_set::GroupingSets,
        operator::{self, LocationRequirement, LogicalNode, LogicalOperator},
    },
};
use rayexec_bullet::{array::Array, batch::Batch, compute::concat::concat, field::TypeSchema};
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};
use std::{collections::HashMap, sync::Arc};

use super::{
    IntermediateOperator, IntermediatePipeline, IntermediatePipelineGroup, IntermediatePipelineId,
    PipelineSource,
};

#[derive(Debug)]
pub struct IntermediatePipelinePlanner {}

impl IntermediatePipelinePlanner {}

#[derive(Debug)]
struct BuildConfig {}

/// Used for ensuring every pipeline in a query has a unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PipelineIdGen {
    gen: IntermediatePipelineId,
}

impl PipelineIdGen {
    fn next(&mut self) -> IntermediatePipelineId {
        let id = self.gen;
        self.gen.0 += 1;
        id
    }
}

#[derive(Debug, Default)]
struct Materializations {
    /// Source pipelines for `MaterializeScan` operators.
    ///
    /// Key corresponds to the index of the materialized plan in the
    /// QueryContext. Since multiple pipelines can read from the same
    /// materialization, each key has a vec of pipelines that we take from.
    materialize_sources: HashMap<usize, Vec<IntermediatePipeline>>,
}

impl Materializations {
    /// Checks if there's any pipelines still in the map.
    ///
    /// This is used as a debugging check. After planning the entire query, all
    /// pending pipelines should have been consumed. If there's still pipelines,
    /// that means we're not accuratately tracking the number of materialized
    /// scans.
    fn has_remaining_pipelines(&self) -> bool {
        for pipelines in self.materialize_sources.values() {
            if !pipelines.is_empty() {
                return true;
            }
        }
        false
    }
}

/// Represents an intermediate pipeline that we're building up.
#[derive(Debug)]
struct InProgressPipeline {
    id: IntermediatePipelineId,
    /// All operators we've planned so far. Should be order left-to-right in
    /// terms of execution flow.
    operators: Vec<IntermediateOperator>,
    /// Location where these operators should be running. This will determine
    /// which pipeline group this pipeline will be placed in.
    location: LocationRequirement,
    /// Source of the pipeline.
    source: PipelineSource,
}

#[derive(Debug)]
struct IntermediatePipelineBuildState {
    /// Pipeline we're working on, as well as the location for where it should
    /// be executed.
    in_progress: Option<InProgressPipeline>,

    local_group: IntermediatePipelineGroup,
    remote_group: IntermediatePipelineGroup,
}

impl IntermediatePipelineBuildState {
    fn new() -> Self {
        IntermediatePipelineBuildState {
            in_progress: None,
            local_group: IntermediatePipelineGroup::default(),
            remote_group: IntermediatePipelineGroup::default(),
        }
    }

    fn walk(
        &mut self,
        conf: &BuildConfig,
        materializations: &mut Materializations,
        id_gen: &mut PipelineIdGen,
        plan: LogicalOperator,
    ) -> Result<()> {
        unimplemented!()
    }

    /// Get the current in-progress pipeline.
    ///
    /// Errors if there's no pipeline in-progress.
    fn in_progress_pipeline_mut(&mut self) -> Result<&mut InProgressPipeline> {
        match &mut self.in_progress {
            Some(pipeline) => Ok(pipeline),
            None => Err(RayexecError::new("No pipeline in-progress")),
        }
    }

    fn take_in_progress_pipeline(&mut self) -> Result<InProgressPipeline> {
        self.in_progress
            .take()
            .ok_or_else(|| RayexecError::new("No in-progress pipeline to take"))
    }

    fn push_as_child_pipeline(&mut self, child: InProgressPipeline) -> Result<()> {
        unimplemented!()
    }

    /// Pushes an intermedate operator onto the in-progress pipeline, erroring
    /// if there is no in-progress pipeline.
    ///
    /// If the location requirement of the operator differs from the in-progress
    /// pipeline, the in-progress pipeline will be finalized and a new
    /// in-progress pipeline created.
    fn push_intermediate_operator(
        &mut self,
        operator: IntermediateOperator,
        location: LocationRequirement,
        id_gen: &mut PipelineIdGen,
    ) -> Result<()> {
        if self
            .in_progress
            .as_ref()
            .required("in-progress pipeline")?
            .location
            != location
        {
            // Different locations, finalize in-progress and start a new one.
            let in_progress = self.take_in_progress_pipeline()?;

            let new_in_progress = InProgressPipeline {
                id: id_gen.next(),
                operators: vec![operator],
                location,
                // TODO: partitions? include other pipeline id
                source: PipelineSource::OtherGroup { partitions: 1 },
            };

            let finalized = IntermediatePipeline {
                id: in_progress.id,
                // TODO: partitions? include other pipeline id
                sink: PipelineSink::OtherGroup { partitions: 1 },
                source: in_progress.source,
                operators: in_progress.operators,
            };

            match in_progress.location {
                LocationRequirement::ClientLocal => {
                    self.local_group.pipelines.insert(finalized.id, finalized);
                }
                LocationRequirement::Remote => {
                    self.remote_group.pipelines.insert(finalized.id, finalized);
                }
                LocationRequirement::Any => {
                    return Err(RayexecError::new("Unexpected any location"))
                }
            }

            self.in_progress = Some(new_in_progress)
        } else {
            // Same location, just push
            let in_progress = self.in_progress_pipeline_mut()?;
            in_progress.operators.push(operator);
        }

        Ok(())
    }

    /// Push a query sink onto the current pipeline. This marks the current
    /// pipeline as completed.
    ///
    /// This is the last step when building up pipelines for a query graph.
    fn push_query_sink(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        sink: QuerySink,
    ) -> Result<()> {
        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalQuerySink),
            partitioning_requirement: Some(sink.num_partitions()),
        };

        // Query sink is always local so that the client can get the results.
        self.push_intermediate_operator(operator, LocationRequirement::ClientLocal, id_gen)?;

        let in_progress = self.take_in_progress_pipeline()?;
        let pipeline = IntermediatePipeline {
            id: in_progress.id,
            sink: PipelineSink::QueryOutput,
            source: in_progress.source,
            operators: in_progress.operators,
        };

        self.local_group.pipelines.insert(pipeline.id, pipeline);

        Ok(())
    }

    fn push_copy_to(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        copy_to: LogicalNode<operator::CopyTo>,
    ) -> Result<()> {
        let location = copy_to.location;
        let copy_to = copy_to.into_inner();

        self.walk(conf, materializations, id_gen, *copy_to.source)?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalCopyTo::new(copy_to.copy_to, copy_to.location)),
            // This should be temporary until there's a better understanding of
            // how we want to handle parallel writes.
            partitioning_requirement: Some(1),
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_set_operation(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        setop: LogicalNode<operator::SetOperation>,
    ) -> Result<()> {
        let location = setop.location;
        let setop = setop.into_inner();

        // Schema from the top. Used as the input to a GROUP BY if ALL is
        // omitted.
        let top_schema = setop.top.output_schema(&[])?;

        // Continue building top.
        self.walk(conf, materializations, id_gen, *setop.top)?;

        // Create new pipelines for bottom.
        let mut bottom_builder = IntermediatePipelineBuildState::new();
        bottom_builder.walk(conf, materializations, id_gen, *setop.bottom)?;
        self.local_group
            .merge_from_other(&mut bottom_builder.local_group);
        self.remote_group
            .merge_from_other(&mut bottom_builder.remote_group);

        let bottom_in_progress = bottom_builder.take_in_progress_pipeline()?;

        match setop.kind {
            operator::SetOpKind::Union => {
                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalUnion),
                    partitioning_requirement: None,
                };

                self.push_intermediate_operator(operator, location, id_gen)?;

                // The union operator is the "sink" for the bottom pipeline.
                self.push_as_child_pipeline(bottom_in_progress)?;
            }
            other => not_implemented!("set op {other}"),
        }

        // Make output distinct by grouping on all columns. No output
        // aggregates, so the output schema remains the same.
        if !setop.all {
            let grouping_sets =
                GroupingSets::new_for_group_by((0..top_schema.types.len()).collect());
            let group_types = top_schema.types;

            let operator = IntermediateOperator {
                operator: Arc::new(PhysicalHashAggregate::new(
                    group_types,
                    grouping_sets,
                    Vec::new(),
                )),
                partitioning_requirement: None,
            };

            self.push_intermediate_operator(operator, location, id_gen)?;
        }

        Ok(())
    }

    fn push_project(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        project: LogicalNode<operator::Projection>,
    ) -> Result<()> {
        let input_schema = project.as_ref().input.output_schema(&[])?;
        let location = project.location;
        let project = project.into_inner();
        self.walk(conf, materializations, id_gen, *project.input)?;

        let projections = project
            .exprs
            .into_iter()
            .map(|expr| PhysicalScalarExpression::try_from_uncorrelated_expr(expr, &input_schema))
            .collect::<Result<Vec<_>>>()?;
        let operator = IntermediateOperator {
            operator: Arc::new(SimpleOperator::new(ProjectOperation::new(projections))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_filter(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        filter: LogicalNode<operator::Filter>,
    ) -> Result<()> {
        let input_schema = filter.as_ref().input.output_schema(&[])?;
        let location = filter.location;
        let filter = filter.into_inner();
        self.walk(conf, materializations, id_gen, *filter.input)?;

        let predicate =
            PhysicalScalarExpression::try_from_uncorrelated_expr(filter.predicate, &input_schema)?;
        let operator = IntermediateOperator {
            operator: Arc::new(SimpleOperator::new(FilterOperation::new(predicate))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_empty(
        &mut self,
        _conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        empty: LogicalNode<()>,
    ) -> Result<()> {
        // "Empty" is a source of data by virtue of emitting a batch consisting
        // of no columns and 1 row.
        //
        // This enables expression evualtion to work without needing to special
        // case a query without a FROM clause. E.g. `SELECT 1+1` would execute
        // the expression `1+1` with the input being the batch with 1 row and no
        // columns.
        //
        // Because this this batch is really just to drive execution on an
        // expression with no input, we just hard the partitions for this
        // pipeline to 1.
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalEmpty),
            partitioning_requirement: None,
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next(),
            operators: vec![operator],
            location: empty.location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn push_values(
        &mut self,
        _conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        values: LogicalNode<operator::ExpressionList>,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        // TODO: This could probably be simplified.

        let location = values.location;
        let values = values.into_inner();

        let mut row_arrs: Vec<Vec<Arc<Array>>> = Vec::new(); // Row oriented.
        let dummy_batch = Batch::empty_with_num_rows(1);

        // Convert expressions into arrays of one element each.
        for row_exprs in values.rows {
            let exprs = row_exprs
                .into_iter()
                .map(|expr| {
                    PhysicalScalarExpression::try_from_uncorrelated_expr(expr, &TypeSchema::empty())
                })
                .collect::<Result<Vec<_>>>()?;
            let arrs = exprs
                .into_iter()
                .map(|expr| expr.eval(&dummy_batch))
                .collect::<Result<Vec<_>>>()?;
            row_arrs.push(arrs);
        }

        let num_cols = row_arrs.first().map(|row| row.len()).unwrap_or(0);
        let mut col_arrs = Vec::with_capacity(num_cols); // Column oriented.

        // Convert the row-oriented vector into a column oriented one.
        for _ in 0..num_cols {
            let cols: Vec<_> = row_arrs.iter_mut().map(|row| row.pop().unwrap()).collect();
            col_arrs.push(cols);
        }

        // Reverse since we worked from right to left when converting to
        // column-oriented.
        col_arrs.reverse();

        // Concat column values into a single array.
        let mut cols = Vec::with_capacity(col_arrs.len());
        for arrs in col_arrs {
            let refs: Vec<&Array> = arrs.iter().map(|a| a.as_ref()).collect();
            let col = concat(&refs)?;
            cols.push(col);
        }

        let batch = Batch::try_new(cols)?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalValues::new(vec![batch])),
            partitioning_requirement: None,
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }
}
