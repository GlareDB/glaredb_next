use rayexec_bullet::field::Field;

use crate::database::create::OnConflict;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::operator::{LogicalOperator, Node};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalCreateSchema {
    pub catalog: String,
    pub name: String,
    pub on_conflict: OnConflict,
}

impl Explainable for LogicalCreateSchema {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateSchema")
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

impl Explainable for LogicalCreateTable {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable")
    }
}
