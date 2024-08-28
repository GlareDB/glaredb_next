use crate::database::drop::DropInfo;

use super::operator::LogicalNode;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDrop {
    pub catalog: String,
    pub info: DropInfo,
}

impl Explainable for LogicalDrop {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Drop")
    }
}
