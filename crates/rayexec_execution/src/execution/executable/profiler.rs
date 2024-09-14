/// Profiler for a single operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorProfiler {
    /// Number of rows read into the operator.
    pub rows_read: usize,
    /// Number of rows produced by the operator.
    pub rows_emitted: usize,
}
