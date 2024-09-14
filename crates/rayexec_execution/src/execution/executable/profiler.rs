use std::time::Duration;

use crate::runtime::time::RuntimeInstant;

#[derive(Debug)]
pub struct PipelineProfileData {
    pub partitions: Vec<PartitionPipelineProfileData>,
}

#[derive(Debug)]
pub struct PartitionPipelineProfileData {
    pub operators: Vec<OperatorProfileData>,
}

#[derive(Debug, Default)]
pub struct OperatorProfileData {
    /// Number of rows read into the operator.
    pub rows_read: usize,
    /// Number of rows produced by the operator.
    pub rows_emitted: usize,
    /// Elapsed time while activley executing this operator.
    pub elapsed: Duration,
}
