use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::{
    expr::Expression,
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef, TableRef},
            expr_binder::{ExpressionBinder, RecursionContext},
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

#[derive(Debug, Clone, PartialEq)]
pub struct BoundValues {
    pub rows: Vec<Vec<Expression>>,
    pub expressions_table: TableRef,
}

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
        _order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
        _limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<BoundValues> {
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

        let first = match rows.first() {
            Some(first) => first,
            None => return Err(RayexecError::new("Empty VALUES statement")),
        };

        let types = first
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        let names = (0..first.len())
            .map(|idx| format!("column{}", idx + 1))
            .collect();

        // TODO: What should happen with limit/order by?

        let table_ref = bind_context.new_ephemeral_table_with_columns(types, names)?;

        Ok(BoundValues {
            rows,
            expressions_table: table_ref,
        })
    }
}
