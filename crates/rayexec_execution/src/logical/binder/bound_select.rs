use std::collections::HashMap;

use crate::{
    expr::Expression,
    logical::{
        binder::{
            bound_from::FromBinder,
            bound_group_by::GroupByBinder,
            bound_modifier::ModifierBinder,
            expr_binder::{ExpressionBinder, RecursionContext},
            select_expr_expander::SelectExprExpander,
            select_list::SelectList,
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::{
    bind_context::{BindContext, BindScopeRef, TableRef},
    bound_from::BoundFrom,
    bound_group_by::BoundGroupBy,
    bound_modifier::{BoundLimit, BoundOrderBy},
    select_list::BoundSelectList,
};

#[derive(Debug, Clone, PartialEq)]
pub struct BoundSelect {
    /// Bound projections.
    pub select_list: BoundSelectList,
    /// Bound FROM.
    pub from: BoundFrom,
    /// Expression for WHERE.
    pub filter: Option<Expression>,
    /// Expression for HAVING.
    pub having: Option<Expression>,
    /// Bound GROUP BY with expressions and grouping sets.
    pub group_by: Option<BoundGroupBy>,
    /// Bound ORDER BY.
    pub order_by: Option<BoundOrderBy>,
    /// Bound LIMIT.
    pub limit: Option<BoundLimit>,
}

#[derive(Debug)]
pub struct SelectBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> SelectBinder<'a> {
    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        select: ast::SelectNode<ResolvedMeta>,
        order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
        limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<BoundSelect> {
        // Handle FROM
        let from_bind_ref = bind_context.new_scope(self.current);
        let from =
            FromBinder::new(from_bind_ref, self.resolve_context).bind(bind_context, select.from)?;

        // Expand SELECT
        let projections =
            SelectExprExpander::new(from_bind_ref, self.resolve_context, bind_context)
                .expand_all_select_exprs(select.projections)?;

        if projections.is_empty() {
            return Err(RayexecError::new("Cannot SELECT * without a FROM clause"));
        }

        let mut select_list = SelectList::try_new(bind_context, projections)?;

        // Track aliases to allow referencing them in GROUP BY and ORDER BY.
        for (idx, projection) in select_list.projections.iter().enumerate() {
            if let Some(alias) = projection.get_alias() {
                select_list
                    .alias_map
                    .insert(alias.as_normalized_string(), idx);
            }
        }

        // Handle WHERE
        let where_expr = select
            .where_expr
            .map(|expr| {
                let binder = ExpressionBinder::new(from_bind_ref, self.resolve_context);
                binder.bind_expression(
                    bind_context,
                    &expr,
                    RecursionContext {
                        allow_window: false,
                        allow_aggregate: false,
                    },
                )
            })
            .transpose()?;

        // Handle ORDER BY, LIMIT, DISTINCT (todo)
        let modifier_binder = ModifierBinder::new(vec![from_bind_ref], self.resolve_context);
        let order_by = order_by
            .map(|order_by| modifier_binder.bind_order_by(bind_context, &mut select_list, order_by))
            .transpose()?;
        let limit = modifier_binder.bind_limit(bind_context, limit)?;

        // Handle GROUP BY
        let group_by = select
            .group_by
            .map(|g| {
                GroupByBinder::new(from_bind_ref, self.resolve_context).bind(
                    bind_context,
                    &mut select_list,
                    g,
                )
            })
            .transpose()?;

        // Handle HAVING
        let having = select
            .having
            .map(|h| {
                ExpressionBinder::new(from_bind_ref, self.resolve_context).bind_expression(
                    bind_context,
                    &h,
                    RecursionContext {
                        allow_aggregate: true,
                        allow_window: false,
                    },
                )
            })
            .transpose()?;

        // Finalize projections.
        let projections = select_list.bind(from_bind_ref, bind_context, self.resolve_context)?;

        let output_columns = select_list.projections.len();

        unimplemented!()
        // Ok(BoundSelect {
        //     select_list,
        //     projections,
        //     output_columns,
        //     from,
        //     filter: where_expr,
        //     having,
        //     group_by,
        //     order_by,
        //     limit,
        //     aggregates: Vec::new(),
        // })
    }
}
