use rayexec_bullet::scalar::OwnedScalarValue;

use crate::engine::vars::SessionVar;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::operator::Node;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalSetVar {
    pub name: String,
    pub value: OwnedScalarValue,
}

impl Explainable for LogicalSetVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Set")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VariableOrAll {
    Variable(SessionVar),
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalResetVar {
    pub var: VariableOrAll,
}

impl Explainable for Node<LogicalResetVar> {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Reset")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalShowVar {
    pub var: SessionVar,
}

impl Explainable for LogicalShowVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Show")
    }
}
