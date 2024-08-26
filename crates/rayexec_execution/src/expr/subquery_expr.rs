use crate::logical::binder::{bind_context::BindScopeRef, bind_query::BoundQuery};

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
