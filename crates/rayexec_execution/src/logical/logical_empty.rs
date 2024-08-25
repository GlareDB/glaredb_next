use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalEmpty;

impl Explainable for LogicalNode<LogicalEmpty> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Empty"), conf)
    }
}
