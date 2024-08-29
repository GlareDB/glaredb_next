use std::collections::BTreeSet;

use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

use super::operator::Node;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAggregate {
    pub aggregates: Vec<Expression>,
    pub group_exprs: Vec<Expression>,
    pub grouping_sets: Option<Vec<BTreeSet<usize>>>,
}

impl Explainable for LogicalAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Aggregate")
    }
}
