use crate::{
    expr::Expression,
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef},
            expr_binder::{ExpressionBinder, RecursionContext},
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::{
    bind_from::{BoundFrom, FromBinder},
    bind_group_by::{BoundGroupBy, GroupByBinder},
    bind_modifier::{BoundLimit, BoundOrderBy, ModifierBinder},
    select_expr_expander::SelectExprExpander,
    select_list::{BoundSelectList, SelectList},
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
        let from_bind_ref = bind_context.new_child_scope(self.current);
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
        let select_list = select_list.bind(from_bind_ref, bind_context, self.resolve_context)?;

        // Move output select columns into current scope.
        match &select_list.pruned {
            Some(pruned) => bind_context.append_table_to_scope(self.current, pruned.table)?,
            None => {
                bind_context.append_table_to_scope(self.current, select_list.projections_table)?
            }
        }

        Ok(BoundSelect {
            select_list,
            from,
            filter: where_expr,
            having,
            group_by,
            order_by,
            limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::DataType;

    use crate::logical::binder::bind_context::testutil::columns_in_scope;

    use super::*;

    #[test]
    fn bind_context_projection_in_scope() {
        let resolve_context = ResolveContext::default();
        let mut bind_context = BindContext::new();

        let binder = SelectBinder {
            current: bind_context.root_scope_ref(),
            resolve_context: &resolve_context,
        };

        let select = ast::SelectNode {
            distinct: None,
            projections: vec![
                ast::SelectExpr::Expr(ast::Expr::Literal(ast::Literal::Number("1".to_string()))),
                ast::SelectExpr::AliasedExpr(
                    ast::Expr::Literal(ast::Literal::Number("1".to_string())),
                    ast::Ident::from_string("my_alias"),
                ),
            ],
            from: None,
            where_expr: None,
            group_by: None,
            having: None,
        };

        let limit = ast::LimitModifier {
            limit: None,
            offset: None,
        };

        let _ = binder.bind(&mut bind_context, select, None, limit).unwrap();

        let cols = columns_in_scope(&bind_context, bind_context.root_scope_ref());
        let expected = vec![
            ("?column?".to_string(), DataType::Int64),
            ("my_alias".to_string(), DataType::Int64),
        ];

        assert_eq!(expected, cols);
    }
}