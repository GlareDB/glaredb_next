use std::collections::HashMap;

use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_bullet::scalar::OwnedScalarValue;

use super::operator::LogicalNode;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAttachDatabase {
    pub datasource: String,
    pub name: String,
    pub options: HashMap<String, OwnedScalarValue>,
}

impl Explainable for LogicalAttachDatabase {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("AttachDatabase")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDetachDatabase {
    pub name: String,
}

impl Explainable for LogicalDetachDatabase {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("DetachDatabase")
    }
}
