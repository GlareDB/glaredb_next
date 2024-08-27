use rayexec_bullet::field::Schema;

use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDescribe {
    pub schema: Schema,
}

impl Explainable for LogicalNode<LogicalDescribe> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Describe"), conf)
    }
}
