use super::{
    explainable::{ExplainConfig, ExplainEntry, Explainable},
    operator::LogicalNode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderByExpr {
    pub expr: usize,
    pub desc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalOrder {
    pub exprs: Vec<OrderByExpr>,
}

impl Explainable for LogicalNode<LogicalOrder> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.annotate_explain(ExplainEntry::new("Order"), conf)
    }
}
