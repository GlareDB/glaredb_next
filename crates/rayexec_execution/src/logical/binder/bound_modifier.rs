use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::logical::{
    binder::expr_binder::ExpressionBinder,
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
pub struct BoundLimit {
    limit: usize,
    offset: Option<usize>,
}

/// Binds ORDER BY, LIMIT, and DISTINCT.
#[derive(Debug)]
pub struct ModifierBinder<'a> {
    /// Contexts in scope.
    ///
    /// Should be a length of 1 for typical select query, and length or two for
    /// set operations.
    pub current: Vec<BindContextIdx>,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ModifierBinder<'a> {
    pub fn new(current: Vec<BindContextIdx>, resolve_context: &'a ResolveContext) -> Self {
        ModifierBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_order_by(
        &self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        order_bys: Vec<ast::OrderByNode<ResolvedMeta>>,
    ) -> Result<BoundOrderBy> {
        // TODO
        let expr_binder =
            ExpressionBinder::new(self.current[0], bind_context, self.resolve_context);

        let exprs = order_bys
            .into_iter()
            .map(|order_by| {
                // Check select list first.
                if let Some(idx) = select_list.get_projection_reference(&order_by.expr)? {
                    // TODO: Return it..
                    unimplemented!()
                }

                let idx = select_list.append_expression(ast::SelectExpr::Expr(order_by.expr));
                // TODO: Do the thing
                unimplemented!()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(BoundOrderBy { exprs })
    }

    pub fn bind_limit(
        &self,
        bind_context: &mut BindContext,
        limit_mod: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<Option<BoundLimit>> {
        let expr_binder =
            ExpressionBinder::new(self.current[0], bind_context, self.resolve_context);

        let limit = match limit_mod.limit {
            Some(limit) => expr_binder.bind_expression(limit)?,
            None => {
                if limit_mod.offset.is_some() {
                    return Err(RayexecError::new("OFFSET without LIMIT not supported"));
                }
                return Ok(None);
            }
        };

        let limit = limit.try_into_scalar()?.try_as_i64()?;
        let limit = if limit < 0 {
            return Err(RayexecError::new("LIMIT cannot be negative"));
        } else {
            limit as usize
        };

        let offset = match limit_mod.offset {
            Some(offset) => {
                let offset = expr_binder.bind_expression(offset)?;
                let offset = offset.try_into_scalar()?.try_as_i64()?;
                if offset < 0 {
                    return Err(RayexecError::new("OFFSET cannot be negative"));
                } else {
                    Some(offset as usize)
                }
            }
            None => None,
        };

        Ok(Some(BoundLimit { limit, offset }))
    }
}
