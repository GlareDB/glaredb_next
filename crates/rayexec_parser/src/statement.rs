use crate::ast::{CreateTable, ExplainNode, Expr, ObjectReference, QueryNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Explain(ExplainNode),

    Query(QueryNode),

    CreateTable(CreateTable),

    /// CREATE SCHEMA ...
    CreateSchema {
        reference: ObjectReference,
        if_not_exists: bool,
    },

    /// SET <variable> TO <value>
    SetVariable {
        reference: ObjectReference,
        value: Expr,
    },

    /// SHOW <variable>
    ShowVariable {
        reference: ObjectReference,
    },
}
