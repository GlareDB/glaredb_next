use crate::expr::Expression;

use super::{
    binder::bind_context::TableRef,
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProject {
    pub projections: Vec<Expression>,
}

impl Explainable for LogicalNode<LogicalProject> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Project"), conf)
    }
}
