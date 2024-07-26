pub mod planner;

use crate::logical::operator::LocationRequirement;

use super::operators::PhysicalOperator;
use std::{collections::HashMap, sync::Arc};

/// ID of a single intermediate pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntermediatePipelineId(pub usize);

#[derive(Debug, Clone)]
pub struct PipelineSink {
    pub pipeline: IntermediatePipelineId,
    pub operator_idx: usize,
    pub location: LocationRequirement,
}

#[derive(Debug)]
pub struct IntermediatePipelineGroup {
    pub(crate) pipelines: HashMap<IntermediatePipelineId, IntermediatePipeline>,
}

#[derive(Debug)]
pub struct IntermediatePipeline {
    pub(crate) id: IntermediatePipelineId,
    pub(crate) location: LocationRequirement,
    pub(crate) sink: PipelineSink,
    pub(crate) operators: Vec<IntermediateOperator>,
}

#[derive(Debug)]
pub struct IntermediateOperator {
    pub(crate) operator: Arc<dyn PhysicalOperator>,
    pub(crate) partitioning_requirement: Option<usize>,
}
