use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::{LogicalNode, LogicalOperator},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalExplain {
    pub analyze: bool,
    pub verbose: bool,
    pub format: ExplainFormat,
    pub logical_unoptimized: Box<LogicalOperator>,
    pub logical_optimized: Option<Box<LogicalOperator>>,
}

impl Explainable for LogicalNode<LogicalExplain> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Explain"), conf)
    }
}
