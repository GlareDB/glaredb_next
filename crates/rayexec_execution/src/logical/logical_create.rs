use rayexec_bullet::field::Field;

use crate::database::create::OnConflict;

use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::{LogicalNode, LogicalOperator},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalCreateSchema {
    pub catalog: String,
    pub name: String,
    pub on_conflict: OnConflict,
}

impl Explainable for LogicalNode<LogicalCreateSchema> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("CreateSchema"), conf)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalCreateTable {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
}

impl Explainable for LogicalNode<LogicalCreateTable> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("CreateTable"), conf)
    }
}
