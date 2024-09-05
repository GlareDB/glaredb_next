use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast;

use crate::{
    expr::{column_expr::ColumnExpr, Expression},
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef},
            column_binder::ExpressionColumnBinder,
            expr_binder::{BaseExpressionBinder, RecursionContext},
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

use super::{bind_group_by::BoundGroupBy, select_list::SelectList};

#[derive(Debug)]
pub struct HavingBinder<'a> {
    column_binder: HavingColumnBinder<'a>,
    current: BindScopeRef,
    resolve_context: &'a ResolveContext,
}

impl<'a> HavingBinder<'a> {
    pub fn new(
        current: BindScopeRef,
        resolve_context: &'a ResolveContext,
        group_by: Option<&'a BoundGroupBy>,
    ) -> Self {
        HavingBinder {
            column_binder: HavingColumnBinder { group_by },
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &mut self,
        bind_context: &mut BindContext,
        having: ast::Expr<ResolvedMeta>,
    ) -> Result<Expression> {
        BaseExpressionBinder::new(self.current, self.resolve_context).bind_expression(
            bind_context,
            &having,
            &mut self.column_binder,
            RecursionContext {
                allow_windows: false,
                allow_aggregates: false, // TODO: Allow true
                is_root: true,
            },
        )
    }
}

// TODO: A bit half-assed. Needs to allow referencing any column used as an
// argument in an aggregate, or column that appears in GROUP BY. Also may itself
// produce aggregates.
#[derive(Debug)]
struct HavingColumnBinder<'a> {
    group_by: Option<&'a BoundGroupBy>,
}

impl<'a> ExpressionColumnBinder for HavingColumnBinder<'a> {
    fn bind_from_root_literal(
        &mut self,
        _bind_scope: BindScopeRef,
        _bind_context: &mut BindContext,
        _literal: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<Expression>> {
        // Can't reference columns by anything other than idents.
        Ok(None)
    }

    fn bind_from_ident(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        ident: &ast::Ident,
        _recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        let col = ident.as_normalized_string();
        let group_by_table = match self.group_by {
            Some(group_by) => group_by.group_table,
            None => {
                not_implemented!("HAVING without GROUP BY")
            }
        };

        not_implemented!("HAVING")
    }

    fn bind_from_idents(
        &mut self,
        bind_scope: BindScopeRef,
        bind_context: &mut BindContext,
        idents: &[ast::Ident],
        _recur: RecursionContext,
    ) -> Result<Option<Expression>> {
        not_implemented!("Compound idents in HAVING")
    }
}
