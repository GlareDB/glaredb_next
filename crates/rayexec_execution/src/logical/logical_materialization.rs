use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::{
    binder::bind_context::{MaterializationRef, TableRef},
    operator::{LogicalNode, Node},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalMaterializationScan {
    pub mat: MaterializationRef,
}

impl Explainable for LogicalMaterializationScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("MaterializationScan").with_value("ref", &self.mat)
    }
}

impl LogicalNode for Node<LogicalMaterializationScan> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        Vec::new()
    }
}
