use crate::logical::binder::{bind_context::BindContextRef, bound_query::BoundQuery};

#[derive(Debug, Clone, PartialEq)]
pub enum SubqueryType {
    Scalar,
    Exists,
    NotExists,
    Any,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryExpr {
    pub bind_idx: BindContextRef,
    pub subquery: BoundQuery,
    pub subquery_type: SubqueryType,
}
