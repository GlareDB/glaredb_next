pub mod bind_from;
pub mod bind_group_by;
pub mod bind_modifier;
pub mod bind_select;
pub mod bind_values;
pub mod select_expr_expander;
pub mod select_list;

use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::resolver::{resolve_context::ResolveContext, ResolvedMeta};
use bind_select::{BoundSelect, SelectBinder};

use super::bind_context::{BindContext, BindScopeRef, TableRef};

#[derive(Debug, Clone, PartialEq)]
pub enum BoundQuery {
    Select(BoundSelect),
}

impl BoundQuery {
    pub fn output_table_ref(&self) -> TableRef {
        match self {
            BoundQuery::Select(select) => match &select.select_list.pruned {
                Some(pruned) => pruned.table,
                None => select.select_list.projections_table,
            },
        }
    }
}

#[derive(Debug)]
pub struct QueryBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> QueryBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        QueryBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        query: ast::QueryNode<ResolvedMeta>,
    ) -> Result<BoundQuery> {
        // TODO: CTEs

        match query.body {
            ast::QueryNodeBody::Select(select) => {
                let binder = SelectBinder::new(self.current, self.resolve_context);
                let select = binder.bind(bind_context, *select, query.order_by, query.limit)?;
                Ok(BoundQuery::Select(select))
            }
            ast::QueryNodeBody::Nested(query) => self.bind(bind_context, *query),
            _ => unimplemented!(),
        }
    }
}
