use crate::{
    execution::{
        intermediate::PipelineSink,
        operators::{
            empty::PhysicalEmpty, filter::FilterOperation, simple::SimpleOperator,
            values::PhysicalValues,
        },
        pipeline::PipelineId,
    },
    expr::PhysicalScalarExpression,
    logical::operator::{self, LocationRequirement, LogicalNode, LogicalOperator},
};
use rayexec_bullet::{array::Array, batch::Batch, compute::concat::concat, field::TypeSchema};
use rayexec_error::{OptionExt, RayexecError, Result};
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
