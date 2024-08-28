use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

use super::{binder::bind_context::TableRef, operator::LogicalNode};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalFilter {
    pub filter: Expression,
}

impl Explainable for LogicalFilter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter")
    }
}
