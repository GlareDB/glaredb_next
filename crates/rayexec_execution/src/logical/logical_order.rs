use super::{binder::bind_query::bind_modifier::BoundOrderByExpr, operator::LogicalNode};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalOrder {
    pub exprs: Vec<BoundOrderByExpr>,
}

impl Explainable for LogicalOrder {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Order")
    }
}
