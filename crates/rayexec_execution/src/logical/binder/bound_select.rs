use std::collections::HashMap;

use crate::logical::{
    binder::{bound_from::FromBinder, expr_binder::ExpressionBinder},
    resolver::{resolve_context::ResolveContext, ResolvedMeta},
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast::{self, SelectExpr};

use super::{
    bind_context::{BindContext, BindContextIdx},
    bound_from::BoundFrom,
};

#[derive(Debug)]
pub struct BoundSelect {
    /// Bound FROM.
    pub from: BoundFrom,
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

        // Track aliases to allow referencing them in GROUP BY and ORDER BY.
        let mut alias_map = HashMap::new();
        for (idx, projection) in projections.iter().enumerate() {
            if let Some(alias) = projection.get_alias() {
                alias_map.insert(alias.as_normalized_string(), idx);
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

        unimplemented!()
    }
}
