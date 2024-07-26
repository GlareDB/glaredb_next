pub mod planner;

use crate::logical::operator::LocationRequirement;

use super::{operators::PhysicalOperator, pipeline::PipelineId};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, Copy)]
pub struct PipelineSink {
    pub pipeline: PipelineId,
    pub operator_idx: usize,
}

#[derive(Debug)]
pub struct IntermediatePipelineGroup {
    pub(crate) pipelines: HashMap<PipelineId, IntermediatePipeline>,
}

#[derive(Debug)]
pub struct IntermediatePipeline {
    pub(crate) id: PipelineId,
    pub(crate) location: LocationRequirement,
    pub(crate) sink: PipelineSink,
    pub(crate) operators: Vec<Arc<dyn PhysicalOperator>>,
}
