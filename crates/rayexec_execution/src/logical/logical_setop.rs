use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node, SetOpKind},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalSetop {
    pub kind: SetOpKind,
    pub all: bool,
    pub table_ref: TableRef,
}

impl Explainable for LogicalSetop {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let kind = format!("{}{}", self.kind, if self.all { " ALL" } else { "" });

        let mut ent = ExplainEntry::new("Setop").with_value("kind", kind);
        if conf.verbose {
            ent = ent.with_value("table_ref", self.table_ref);
        }

        ent
    }
}

impl LogicalNode for Node<LogicalSetop> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        vec![self.node.table_ref]
    }
}
