use std::collections::HashMap;

use crate::{
    expr::Expression,
    logical::{
        binder::{
            bound_from::FromBinder, bound_group_by::GroupByBinder, bound_modifier::ModifierBinder,
            expr_binder::ExpressionBinder, select_list::SelectList,
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::{
    bind_context::{BindContext, BindContextRef},
    bound_from::BoundFrom,
    bound_group_by::BoundGroupBy,
    bound_modifier::{BoundLimit, BoundOrderBy},
};

#[derive(Debug)]
pub struct BoundSelect {
    /// Unplanned projections in select, including appended projections.
    pub select_list: SelectList,
    /// Planned expressions in the select.
    pub projections: Vec<Expression>,
    /// Number of columns in the output.
    ///
    /// This may be greater than len(projections) in order handle
    /// pre-projections into ORDER BY and GROUP BY. If this is the case,
    /// those appended columns should be omitted at the end.
    pub output_columns: usize,
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
    /// Any aggregates in the select list.
    pub aggregates: Vec<Expression>,
}

#[derive(Debug)]
pub struct SelectBinder<'a> {
    pub current: BindContextRef,
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
        let from =
            FromBinder::new(self.current, self.resolve_context).bind(bind_context, select.from)?;

        // Expand SELECT
        let projections = ExpressionBinder::new(self.current, self.resolve_context)
            .expand_all_select_exprs(bind_context, select.projections)?;

        if projections.is_empty() {
            return Err(RayexecError::new("Cannot SELECT * without a FROM clause"));
        }

        let mut select_list = SelectList {
            table: bind_context.push_empty_scope(self.current)?,
            alias_map: HashMap::new(),
            projections,
            appended: Vec::new(),
        };

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
                let binder = ExpressionBinder::new(self.current, self.resolve_context);
                binder.bind_expression(bind_context, expr)
            })
            .transpose()?;

        // Handle ORDER BY, LIMIT, DISTINCT (todo)
        let modifier_binder = ModifierBinder::new(vec![self.current], self.resolve_context);
        let order_by = order_by
            .map(|order_by| modifier_binder.bind_order_by(bind_context, &mut select_list, order_by))
            .transpose()?;
        let limit = modifier_binder.bind_limit(bind_context, limit)?;

        // Handle GROUP BY
        let group_by = select
            .group_by
            .map(|g| {
                GroupByBinder::new(self.current, self.resolve_context).bind(
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
                ExpressionBinder::new(self.current, self.resolve_context)
                    .bind_expression(bind_context, h)
            })
            .transpose()?;

        unimplemented!()
    }
}
