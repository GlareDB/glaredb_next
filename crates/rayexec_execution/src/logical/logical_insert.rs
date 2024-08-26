use std::sync::Arc;

use crate::database::catalog_entry::CatalogEntry;

use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::{LogicalNode, LogicalOperator},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalInsert {
    pub catalog: String,
    pub schema: String,
    pub table: Arc<CatalogEntry>,
}

impl Explainable for LogicalNode<LogicalInsert> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Insert"), conf)
    }
}
