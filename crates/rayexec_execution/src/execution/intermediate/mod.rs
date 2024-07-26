use crate::logical::operator::LocationRequirement;

use super::operators::PhysicalOperator;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct SinkIndex {
    pub pipeline_idx: usize,
    pub operator_idx: usize,
}

#[derive(Debug)]
pub struct IntermediatePipelineGroup {
    pub(crate) pipelines: Vec<Arc<dyn PhysicalOperator>>,
}

#[derive(Debug)]
pub struct IntermediatePipeline {
    pub(crate) location: LocationRequirement,
    pub(crate) sink: SinkIndex,
    pub(crate) operators: Vec<Arc<dyn PhysicalOperator>>,
}
