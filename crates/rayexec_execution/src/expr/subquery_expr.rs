use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use crate::logical::binder::{
    bind_context::{BindContext, BindScopeRef},
    bind_query::BoundQuery,
};
use std::fmt;

use super::{comparison_expr::ComparisonOperator, Expression};

#[derive(Debug, Clone, PartialEq)]
pub enum SubqueryType {
    Scalar,
    Exists {
        negated: bool,
    },
    Any {
        /// Expression for ANY/IN/ALL subqueries
        ///
        /// ... WHERE <expr> > ALL (<subquery>) ...
        expr: Box<Expression>,
        /// The comparison operator to use.
        op: ComparisonOperator,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryExpr {
    pub bind_idx: BindScopeRef,
    pub subquery: Box<BoundQuery>,
    pub subquery_type: SubqueryType,
    pub return_type: DataType,
}

impl SubqueryExpr {
    pub fn has_correlations(&self, bind_context: &BindContext) -> Result<bool> {
        let cols = bind_context.correlated_columns(self.bind_idx)?;
        Ok(!cols.is_empty())
    }
}

impl fmt::Display for SubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.subquery_type {
            SubqueryType::Scalar => (),
            SubqueryType::Exists { negated: false } => write!(f, "EXISTS ")?,
            SubqueryType::Exists { negated: true } => write!(f, "NOT EXISTS ")?,
            SubqueryType::Any { expr, op } => write!(f, "{expr} {op} ANY ")?,
        }

        write!(f, "<subquery>")
    }
}
