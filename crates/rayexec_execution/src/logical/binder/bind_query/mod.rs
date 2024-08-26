pub mod bind_from;
pub mod bind_group_by;
pub mod bind_modifier;
pub mod bind_select;
pub mod select_expr_expander;
pub mod select_list;

use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::resolver::{resolve_context::ResolveContext, ResolvedMeta};
use bind_select::{BoundSelect, SelectBinder};

use super::bind_context::{BindContext, BindScopeRef};

#[derive(Debug, Clone, PartialEq)]
pub enum BoundQuery {
    Select(BoundSelect),
}

#[derive(Debug)]
pub struct QueryBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> QueryBinder<'a> {
    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        query: ast::QueryNode<ResolvedMeta>,
    ) -> Result<BoundQuery> {
        // TODO: CTEs

        match query.body {
            ast::QueryNodeBody::Select(select) => {
                let binder = SelectBinder {
                    current: self.current,
                    resolve_context: self.resolve_context,
                };
                let select = binder.bind(bind_context, *select, query.order_by, query.limit)?;
                Ok(BoundQuery::Select(select))
            }
            _ => unimplemented!(),
        }
    }
}
