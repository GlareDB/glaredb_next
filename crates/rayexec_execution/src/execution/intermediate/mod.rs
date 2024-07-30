pub mod planner;

use crate::logical::operator::LocationRequirement;

use super::operators::PhysicalOperator;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

/// ID of a single intermediate pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntermediatePipelineId(pub usize);

/// Location of the sink for a particular pipeline.
///
/// During single-node execution this will point to an operator where batches
/// should be pushed to (e.g. the build side of a join).
///
/// Hyrbid execution introduces a chance for the sink to be a remote pipeline.
/// To handle this, we insert an ipc sink/source operator on both ends. The
/// `Remote` variant contians information for building the sink side
/// appropriately.
#[derive(Debug, Clone)]
pub enum PipelineSink {
    /// The pipeline's sink is the output of the query.
    QueryOutput,
    /// Sink is in the same group of operators as itself.
    InGroup {
        pipeline_id: IntermediatePipelineId,
        operator_idx: usize,
        input_idx: usize,
    },
    /// Sink is a pipeline executing remotely.
    OtherGroup { partitions: usize },
}

/// Location of the source of a pipeline.
///
/// Single-node execution will always have the source as the first operator in
/// the chain (and nothing needs to be done).
///
/// For hybrid execution, the source may be a remote pipeline, and so we will
/// include an ipc source operator as this pipeline's source.
#[derive(Debug, Clone)]
pub enum PipelineSource {
    /// Source is already in the pipeline, don't do anything.
    InPipeline,
    /// Source is remote, build an ipc source.
    OtherGroup { partitions: usize },
}

#[derive(Debug, Default)]
pub struct IntermediatePipelineGroup {
    pub(crate) pipelines: HashMap<IntermediatePipelineId, IntermediatePipeline>,
}

impl IntermediatePipelineGroup {
    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty()
    }

    pub fn merge_from_other(&mut self, other: &mut Self) {
        self.pipelines.extend(other.pipelines.drain())
    }
}

#[derive(Debug)]
pub struct IntermediatePipeline {
    pub(crate) id: IntermediatePipelineId,
    pub(crate) sink: PipelineSink,
    pub(crate) source: PipelineSource,
    pub(crate) operators: Vec<IntermediateOperator>,
}

#[derive(Debug)]
pub struct IntermediateOperator {
    /// The physical operator that will be used in the executable pipline.
    pub(crate) operator: Arc<dyn PhysicalOperator>,
    /// Which index is considered the "trunk" as data flows through this
    /// operator.
    ///
    /// For single input operators, this is always 0. For joins, the build side
    /// is typically index 0, and the probe side is index 1. We consider the the
    /// probe side to be the trunk, so this would be set to 1.
    pub(crate) trunk_idx: usize,
    /// If this operator has a partitioning requirement.
    ///
    /// If set, the input and output partitions for this operator will be the
    /// value provided. If unset, it'll default to a value determeded by the
    /// executable pipeline planner.
    pub(crate) partitioning_requirement: Option<usize>,
}
