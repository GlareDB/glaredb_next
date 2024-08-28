use super::operator::{LogicalNode, LogicalOperator};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

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

impl Explainable for LogicalExplain {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Explain")
    }
}
