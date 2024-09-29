use crate::expr::Expression;

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
    statistics::Statistics,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProject {
    pub projections: Vec<Expression>,
    pub projection_table: TableRef,
}

impl Explainable for LogicalProject {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Project").with_values_context(
            "projections",
            conf,
            &self.projections,
        );

        if conf.verbose {
            ent = ent.with_value("table_ref", self.projection_table)
        }

        ent
    }
}

impl LogicalNode for Node<LogicalProject> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        vec![self.node.projection_table]
    }

    fn get_statistics(&self) -> Statistics {
        self.iter_child_statistics()
            .next()
            .expect("single child for project")
    }
}
