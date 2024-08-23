use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalLimit {
    pub offset: Option<usize>,
    pub limit: usize,
}

impl Explainable for LogicalNode<LogicalLimit> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Limit"), conf)
    }
}
