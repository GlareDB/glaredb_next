use rayexec_bullet::scalar::OwnedScalarValue;

use crate::engine::vars::SessionVar;

use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalSetVar {
    pub name: String,
    pub value: OwnedScalarValue,
}

impl Explainable for LogicalNode<LogicalSetVar> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Set"), conf)
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

impl Explainable for LogicalNode<LogicalResetVar> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Reset"), conf)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalShowVar {
    pub var: SessionVar,
}

impl Explainable for LogicalNode<LogicalShowVar> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Show"), conf)
    }
}
