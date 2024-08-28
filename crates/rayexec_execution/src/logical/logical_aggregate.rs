use std::collections::BTreeSet;

use crate::expr::Expression;

use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAggregate {
    pub aggregates: Vec<Expression>,
    pub group_exprs: Vec<Expression>,
    pub grouping_sets: Option<Vec<BTreeSet<usize>>>,
}

impl Explainable for LogicalNode<LogicalAggregate> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Scan"), conf)
    }
}
