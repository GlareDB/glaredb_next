use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;
use std::fmt;

use crate::{
    expr::{column_expr::ColumnExpr, Expression},
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef},
            column_binder::DefaultColumnBinder,
            expr_binder::{ExpressionBinder, RecursionContext},
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

use super::select_list::SelectList;

#[derive(Debug, Clone, PartialEq)]
pub struct BoundOrderByExpr {
    pub expr: Expression,
    pub desc: bool,
    pub nulls_first: bool,
}

impl fmt::Display for BoundOrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.expr,
            if self.desc { "desc" } else { "asc" },
            if self.nulls_first {
                "nulls_first"
            } else {
                "nulls_last"
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundOrderBy {
    pub exprs: Vec<BoundOrderByExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundLimit {
    pub limit: usize,
    pub offset: Option<usize>,
}

/// Binds ORDER BY, LIMIT, and DISTINCT.
#[derive(Debug)]
pub struct ModifierBinder<'a> {
    /// Contexts in scope.
    ///
    /// Should be a length of 1 for typical select query, and length or two for
    /// set operations.
    pub current: Vec<BindScopeRef>,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ModifierBinder<'a> {
    pub fn new(current: Vec<BindScopeRef>, resolve_context: &'a ResolveContext) -> Self {
        ModifierBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_order_by(
        &self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        order_by: ast::OrderByModifier<ResolvedMeta>,
    ) -> Result<BoundOrderBy> {
        let exprs = order_by
            .order_by_nodes
            .into_iter()
            .map(|order_by| {
                let expr = Expression::Column(self.bind_to_select_list(
                    bind_context,
                    select_list,
                    order_by.expr,
                )?);

                Ok(BoundOrderByExpr {
                    expr,
                    desc: matches!(
                        order_by.typ.unwrap_or(ast::OrderByType::Asc),
                        ast::OrderByType::Desc
                    ),
                    nulls_first: matches!(
                        order_by.nulls.unwrap_or(ast::OrderByNulls::First),
                        ast::OrderByNulls::First
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(BoundOrderBy { exprs })
    }

    /// Generates a column epxression referencing the output of the select list.
    fn bind_to_select_list(
        &self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        expr: ast::Expr<ResolvedMeta>,
    ) -> Result<ColumnExpr> {
        // Check if there's already something in the list that we're
        // referencing.
        if let Some(expr) = select_list.column_expr_for_reference(bind_context, &expr)? {
            return Ok(expr);
        }

        // TODO: What do here?
        let current = match self.current.first() {
            Some(&current) => current,
            None => return Err(RayexecError::new("Missing scope, cannot bind to anything")),
        };

        let expr = ExpressionBinder::new(current, self.resolve_context).bind_expression(
            bind_context,
            &expr,
            &mut DefaultColumnBinder::new(current),
            RecursionContext {
                allow_window: false,
                allow_aggregate: false,
            },
        )?;

        select_list.append_expression(bind_context, expr)
    }

    pub fn bind_limit(
        &self,
        bind_context: &mut BindContext,
        limit_mod: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<Option<BoundLimit>> {
        // TODO: What do here?
        let current = match self.current.first() {
            Some(&current) => current,
            None => return Err(RayexecError::new("Missing scope, cannot bind to anything")),
        };

        let expr_binder = ExpressionBinder::new(current, self.resolve_context);

        let limit = match limit_mod.limit {
            Some(limit) => expr_binder.bind_expression(
                bind_context,
                &limit,
                &mut DefaultColumnBinder::new(self.current[0]),
                RecursionContext {
                    allow_window: false,
                    allow_aggregate: false,
                },
            )?,
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
                let offset = expr_binder.bind_expression(
                    bind_context,
                    &offset,
                    &mut DefaultColumnBinder::new(self.current[0]),
                    RecursionContext {
                        allow_window: false,
                        allow_aggregate: false,
                    },
                )?;
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
