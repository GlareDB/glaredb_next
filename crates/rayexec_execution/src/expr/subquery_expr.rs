use crate::logical::binder::{bind_context::BindScopeRef, bind_query::BoundQuery};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum SubqueryType {
    Scalar,
    Exists,
    NotExists,
    Any,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryExpr {
    pub bind_idx: BindScopeRef,
    pub subquery: Box<BoundQuery>,
    pub subquery_type: SubqueryType,
}

impl fmt::Display for SubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO
        write!(f, "<subquery>")
    }
}
