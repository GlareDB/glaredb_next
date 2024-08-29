use crate::expr::Expression;

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProject {
    pub projections: Vec<Expression>,
    pub projection_table: TableRef,
}

impl Explainable for LogicalProject {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Project")
    }
}

impl LogicalNode for Node<LogicalProject> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        vec![self.node.projection_table]
    }
}
