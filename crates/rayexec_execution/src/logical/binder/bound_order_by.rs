use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::{
    expr::LogicalExpression,
    resolver::{resolve_context::ResolveContext, ResolvedMeta},
};

use super::{
    bind_context::{BindContext, BindContextIdx},
    select_list::SelectList,
};

#[derive(Debug)]
pub struct BoundOrderByExpr {
    pub expr: LogicalExpression,
    pub desc: bool,
    pub nulls_first: bool,
}

#[derive(Debug)]
pub struct BoundOrderBy {
    pub exprs: Vec<BoundOrderByExpr>,
}

#[derive(Debug)]
pub struct OrderByBinder<'a> {
    /// Contexts in scope.
    ///
    /// Should be a length of 1 for typical select query, and length or two for
    /// set operations.
    pub current: Vec<BindContextIdx>,
    pub resolve_context: &'a ResolveContext,
    pub select_list: &'a SelectList,
}

impl<'a> OrderByBinder<'a> {
    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        order_bys: Vec<ast::OrderByNode<ResolvedMeta>>,
    ) -> Result<BoundOrderBy> {
        unimplemented!()
    }
}
