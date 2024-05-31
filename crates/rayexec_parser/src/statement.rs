use crate::ast::{
    CreateSchema, CreateTable, ExplainNode, Expr, Insert, ObjectReference, QueryNode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Explain(ExplainNode),

    Query(QueryNode),

    CreateTable(CreateTable),

    Insert(Insert),

    /// CREATE SCHEMA ...
    CreateSchema(CreateSchema),

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
