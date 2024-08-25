use rayexec_error::Result;
use rayexec_parser::{ast, statement::Statement};

use crate::{
    engine::vars::SessionVars,
    logical::{
        binder::bound_query::QueryBinder,
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

use super::{bind_context::BindContext, bound_query::BoundQuery};

#[derive(Debug)]
pub enum BoundStatement {
    Query(BoundQuery),
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
        let current_scope = context.root_scope_ref();

        let statement = match statement {
            Statement::Query(query) => {
                let binder = QueryBinder {
                    current: current_scope,
                    resolve_context: self.resolve_context,
                };
                BoundStatement::Query(binder.bind(&mut context, query)?)
            }
            _ => unimplemented!(),
        };

        Ok((statement, context))
    }
}
