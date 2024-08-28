use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::logical::{
    binder::{
        bind_context::{BindContext, BindScopeRef},
        expr_binder::{ExpressionBinder, RecursionContext},
    },
    resolver::{resolve_context::ResolveContext, ResolvedMeta},
};

#[derive(Debug)]
pub struct BoundValues {}

#[derive(Debug)]
pub struct ValuesBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ValuesBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        ValuesBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        values: ast::Values<ResolvedMeta>,
        order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
        limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<BoundValues> {
        if values.rows.is_empty() {
            return Err(RayexecError::new("Empty VALUES expression"));
        }

        // TODO: This could theoretically bind expressions as correlated
        // columns. TBD if that's desired.
        let expr_binder = ExpressionBinder::new(self.current, self.resolve_context);
        let rows = values
            .rows
            .into_iter()
            .map(|row| {
                expr_binder.bind_expressions(
                    bind_context,
                    &row,
                    RecursionContext {
                        allow_window: false,
                        allow_aggregate: false,
                    },
                )
            })
            .collect::<Result<Vec<Vec<_>>>>()?;

        // TODO: What should happen with limit/order by?

        unimplemented!()
    }
}
