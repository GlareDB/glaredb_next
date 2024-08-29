use super::operator::Node;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalLimit {
    pub offset: Option<usize>,
    pub limit: usize,
}

impl Explainable for LogicalLimit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Limit")
    }
}
