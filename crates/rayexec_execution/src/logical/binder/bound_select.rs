use std::collections::HashMap;

use crate::logical::{
    binder::{
        bound_from::FromBinder, bound_modifier::ModifierBinder, expr_binder::ExpressionBinder,
        select_list::SelectList,
    },
    expr::LogicalExpression,
    resolver::{resolve_context::ResolveContext, ResolvedMeta},
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::{
    bind_context::{BindContext, BindContextIdx},
    bound_from::BoundFrom,
};

#[derive(Debug)]
pub struct BoundSelect {
    /// Projections in select, including appended projections.
    pub select_list: SelectList,
    /// Bound FROM.
    pub from: BoundFrom,
    /// Expression for WHERE.
    pub filter: Option<LogicalExpression>,
    /// Expression for HAVING.
    pub having: Option<LogicalExpression>,
    /// Any aggregates in the select list.
    pub aggregates: Vec<LogicalExpression>,
}

#[derive(Debug)]
pub struct SelectBinder<'a> {
    pub current: BindContextIdx,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> SelectBinder<'a> {
    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        select: ast::SelectNode<ResolvedMeta>,
        order_by: Vec<ast::OrderByNode<ResolvedMeta>>,
        limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<Self> {
        // Handle FROM
        let from =
            FromBinder::new(self.current, self.resolve_context).bind(bind_context, select.from)?;

        // Expand SELECT
        let projections = ExpressionBinder::new(self.current, bind_context, self.resolve_context)
            .expand_all_select_exprs(select.projections)?;

        if projections.is_empty() {
            return Err(RayexecError::new("Cannot SELECT * without a FROM clause"));
        }

        let mut select_list = SelectList {
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
                let binder =
                    ExpressionBinder::new(self.current, bind_context, self.resolve_context);
                binder.bind_expression(expr)
            })
            .transpose()?;

        // Handle ORDER BY, LIMIT, DISTINCT (todo)
        let modifier_binder = ModifierBinder::new(vec![self.current], self.resolve_context);
        let order_by = modifier_binder.bind_order_by(bind_context, &mut select_list, order_by)?;
        let limit = modifier_binder.bind_limit(bind_context, limit)?;

        unimplemented!()
    }
}
