use super::pipeline::PartitionPipelineInfo;

#[derive(Debug, Clone)]
pub struct PartitionPipelineMetrics {
    /// Info for identifying which partition/pipeline these metrics are for.
    pub info: PartitionPipelineInfo,

    /// Metrics for each of the operators in the pipeline.
    pub operator_metrics: Vec<OperatorMetrics>,
}

#[derive(Debug, Clone, Default)]
pub struct OperatorMetrics {
    pub pull_metrics: PullMetrics,
    pub push_metrics: PushMetrics,
}

#[derive(Debug, Clone, Default)]
pub struct PullMetrics {
    /// Total number of pulls from this operator.
    pub total_pulls: usize,

    /// Number of times we've gottening pending.
    pub pending_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct PushMetrics {
    /// Total number of pushes to the operator.
    pub total_pushes: usize,

    /// Number of times we've gotten a pending.
    pub pending_count: usize,
}
