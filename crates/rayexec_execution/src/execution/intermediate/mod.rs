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

#[derive(Debug)]
pub struct IntermediatePipelineGroup {
    pub(crate) pipelines: HashMap<IntermediatePipelineId, IntermediatePipeline>,
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
    pub(crate) operator: Arc<dyn PhysicalOperator>,
    pub(crate) partitioning_requirement: Option<usize>,
}
