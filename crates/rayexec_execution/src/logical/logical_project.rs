use crate::expr::Expression;

use super::{binder::bind_context::TableRef, operator::LogicalNode};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProject {
    pub projections: Vec<Expression>,
}

impl Explainable for LogicalProject {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Project")
    }
}
