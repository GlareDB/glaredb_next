use rayexec_parser::ast;
use rayexec_error::{ Result, RayexecError };

use crate::{expr::Expression, logical::resolver::ResolvedMeta};

use super::{
    bind_context::BindContext,
    bound_select::BoundSelect,
    expr_binder::ExpressionBinder,
    select_list::{BoundSelectList, SelectList},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecursionContext {
    pub in_aggregate: bool,
}

#[derive(Debug)]
pub struct SelectExprBinder<'a> {
    /// Base expression binder.
    binder: ExpressionBinder<'a>,
}

impl<'a> SelectExprBinder<'a> {
    pub fn bind_expression(
        &self,
        expr: &ast::Expr<ResolvedMeta>,
        bind_context: &mut BindContext,
        select: &mut BoundSelect,
        recur: RecursionContext,
    ) -> Result<Expression> {
        unimplemented!()
        // match expr {
        //     ast::Expr::Ident(ident) => {
        //         // TODO: Group by alias
        //         self.binder.bind_ident(bind_context, ident)
        //     }
        //     ast::Expr::Function(func) => self.bind_function(bind_context, func, select, recur),
        //     other => self.binder.bind_expression(bind_context, other),
        // }
    }

    pub(crate) fn bind_function(
        &self,
        bind_context: &mut BindContext,
        expr: &ast::Function<ResolvedMeta>,
        select: &mut BoundSelect,
        recur: RecursionContext,
    ) -> Result<Expression> {
        unimplemented!()
    }
}
