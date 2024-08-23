use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalProject;

impl Explainable for LogicalNode<LogicalProject> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Project"), conf)
    }
}
