use std::collections::BTreeSet;

use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAggregate {
    pub aggregates: Vec<usize>,
    pub group_exprs: Vec<usize>,
    pub grouping_sets: Vec<BTreeSet<usize>>,
}

impl Explainable for LogicalNode<LogicalAggregate> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Scan"), conf)
    }
}
