use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalFilter;

impl Explainable for LogicalNode<LogicalFilter> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Filter"), conf)
    }
}
