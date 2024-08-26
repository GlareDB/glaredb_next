use super::{
    binder::bind_query::bind_modifier::BoundOrderByExpr,
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalOrder {
    pub exprs: Vec<BoundOrderByExpr>,
}

impl Explainable for LogicalNode<LogicalOrder> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Order"), conf)
    }
}
