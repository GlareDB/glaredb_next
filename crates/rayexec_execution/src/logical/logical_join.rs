use crate::{
    explain::explainable::{ExplainConfig, ExplainEntry, Explainable},
    expr::{comparison_expr::ComparisonOperator, Expression},
};
use std::fmt;

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Left,
    Right,
    Inner,
    Full,
    Semi,
    Anti,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner => write!(f, "INNER"),
            Self::Left => write!(f, "LEFT"),
            Self::Right => write!(f, "RIGHT"),
            Self::Full => write!(f, "FULL"),
            Self::Semi => write!(f, "SEMI"),
            Self::Anti => write!(f, "ANTI"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComparisonCondition {
    /// Expression containing column references from the left side.
    pub left: Expression,
    /// Expression containing column references from the right side.
    pub right: Expression,
    /// Comparision operator.
    pub op: ComparisonOperator,
}

impl ComparisonCondition {
    pub fn flip_sides(&mut self) {
        self.op = match self.op {
            ComparisonOperator::Eq => ComparisonOperator::Eq,
            ComparisonOperator::NotEq => ComparisonOperator::NotEq,
            ComparisonOperator::Lt => ComparisonOperator::Gt,
            ComparisonOperator::LtEq => ComparisonOperator::LtEq,
            ComparisonOperator::Gt => ComparisonOperator::Lt,
            ComparisonOperator::GtEq => ComparisonOperator::GtEq,
        };
        std::mem::swap(&mut self.left, &mut self.right);
    }
}

impl fmt::Display for ComparisonCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalComparisonJoin {
    pub join_type: JoinType,
    pub conditions: Vec<ComparisonCondition>,
}

impl Explainable for LogicalComparisonJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ComparisonJoin")
            .with_values("conditions", &self.conditions)
            .with_value("join_type", self.join_type)
    }
}

impl LogicalNode for Node<LogicalComparisonJoin> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.get_children_table_refs()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalArbitraryJoin {
    pub join_type: JoinType,
    pub condition: Expression,
}

impl Explainable for LogicalArbitraryJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ArbitraryJoin").with_value("join_type", self.join_type)
    }
}

impl LogicalNode for Node<LogicalArbitraryJoin> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.get_children_table_refs()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalCrossJoin;

impl Explainable for LogicalCrossJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CrossJoin")
    }
}

impl LogicalNode for Node<LogicalCrossJoin> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.get_children_table_refs()
    }
}
