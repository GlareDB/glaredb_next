use rayexec_bullet::field::Schema;

use super::operator::Node;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDescribe {
    pub schema: Schema,
}

impl Explainable for LogicalDescribe {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Describe")
    }
}
