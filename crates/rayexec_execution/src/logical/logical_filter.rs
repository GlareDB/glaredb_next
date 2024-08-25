use crate::expr::Expression;

use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalFilter {
    pub filter: Expression,
}

impl Explainable for LogicalNode<LogicalFilter> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Filter"), conf)
    }
}
