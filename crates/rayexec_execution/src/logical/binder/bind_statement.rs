use rayexec_error::Result;
use rayexec_parser::{ast, statement::Statement};

use crate::{
    engine::vars::SessionVars,
    logical::{
        binder::bind_query::QueryBinder,
        logical_drop::LogicalDrop,
        logical_set::{LogicalResetVar, LogicalSetVar, LogicalShowVar},
        operator::LogicalNode,
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

use super::{
    bind_attach::{AttachBinder, BoundAttach, BoundDetach},
    bind_context::BindContext,
    bind_drop::DropBinder,
    bind_insert::BoundInsert,
    bind_query::BoundQuery,
    bind_set::SetVarBinder,
};

#[derive(Debug)]
pub enum BoundStatement {
    Query(BoundQuery),
    SetVar(LogicalNode<LogicalSetVar>),
    ResetVar(LogicalNode<LogicalResetVar>),
    ShowVar(LogicalNode<LogicalShowVar>),
    Attach(BoundAttach),
    Detach(BoundDetach),
    Drop(LogicalNode<LogicalDrop>),
    Insert(BoundInsert),
}

#[derive(Debug)]
pub struct StatementBinder<'a> {
    pub session_vars: &'a SessionVars,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> StatementBinder<'a> {
    pub fn bind(
        &self,
        statement: Statement<ResolvedMeta>,
    ) -> Result<(BoundStatement, BindContext)> {
        let mut context = BindContext::new();
        let root_scope = context.root_scope_ref();

        let statement = match statement {
            Statement::Query(query) => {
                let binder = QueryBinder {
                    current: root_scope,
                    resolve_context: self.resolve_context,
                };
                BoundStatement::Query(binder.bind(&mut context, query)?)
            }
            Statement::SetVariable(set) => BoundStatement::SetVar(
                SetVarBinder::new(root_scope, self.session_vars).bind_set(&mut context, set)?,
            ),
            Statement::ShowVariable(set) => BoundStatement::ShowVar(
                SetVarBinder::new(root_scope, self.session_vars).bind_show(&mut context, set)?,
            ),
            Statement::ResetVariable(set) => BoundStatement::ResetVar(
                SetVarBinder::new(root_scope, self.session_vars).bind_reset(&mut context, set)?,
            ),
            Statement::Attach(attach) => BoundStatement::Attach(
                AttachBinder::new(root_scope).bind_attach(&mut context, attach)?,
            ),
            Statement::Detach(detach) => BoundStatement::Detach(
                AttachBinder::new(root_scope).bind_detach(&mut context, detach)?,
            ),
            Statement::Drop(drop) => {
                BoundStatement::Drop(DropBinder::new(root_scope).bind_drop(&mut context, drop)?)
            }
            _ => unimplemented!(),
        };

        Ok((statement, context))
    }
}
