use std::collections::HashMap;

use crate::logical::{
    binder::expr_binder::ExpressionBinder,
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
    /// Mapping from explicit user-provided alias to column index in the output.
    pub alias_map: HashMap<String, usize>,
    /// Bound FROM.
    pub from: BoundFrom,
}

impl BoundSelect {
    pub fn bind(
        current: BindContextIdx,
        bind_context: &mut BindContext,
        resolve_context: &ResolveContext,
        select: ast::SelectNode<ResolvedMeta>,
    ) -> Result<Self> {
        // Handle FROM
        let from = BoundFrom::bind(current, bind_context, resolve_context, select.from)?;

        // Expand SELECT
        let projections = ExpressionBinder::new(current, bind_context, resolve_context)
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
                let binder = ExpressionBinder::new(current, bind_context, resolve_context);
                binder.bind_expression(expr)
            })
            .transpose()?;

        unimplemented!()
    }
}
