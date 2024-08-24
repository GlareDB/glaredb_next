use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::{
    expr::{column_expr::ColumnExpr, Expression},
    logical::{
        binder::bind_context::CorrelatedColumn,
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

use super::bind_context::{BindContext, BindContextRef};

#[derive(Debug)]
pub struct ExpressionBinder<'a> {
    pub current: BindContextRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ExpressionBinder<'a> {
    pub const fn new(current: BindContextRef, resolve_context: &'a ResolveContext) -> Self {
        ExpressionBinder {
            current,
            resolve_context,
        }
    }

    pub fn expand_all_select_exprs(
        &self,
        bind_context: &BindContext,
        exprs: impl IntoIterator<Item = ast::SelectExpr<ResolvedMeta>>,
    ) -> Result<Vec<ast::SelectExpr<ResolvedMeta>>> {
        let mut expanded = Vec::new();
        for expr in exprs {
            let mut ex = self.expand_select_expr(bind_context, expr)?;
            expanded.append(&mut ex);
        }
        Ok(expanded)
    }

    pub fn expand_select_expr(
        &self,
        bind_context: &BindContext,
        expr: ast::SelectExpr<ResolvedMeta>,
    ) -> Result<Vec<ast::SelectExpr<ResolvedMeta>>> {
        Ok(match expr {
            ast::SelectExpr::Wildcard(_wildcard) => {
                // TODO: Exclude, replace
                let mut exprs = Vec::new();
                for scope in bind_context.iter_table_scopes(self.current)? {
                    let table = &scope.alias;
                    for column in &scope.column_names {
                        exprs.push(ast::SelectExpr::Expr(ast::Expr::CompoundIdent(vec![
                            ast::Ident::from_string(table),
                            ast::Ident::from_string(column),
                        ])))
                    }
                }

                exprs
            }
            ast::SelectExpr::QualifiedWildcard(reference, _wildcard) => {
                // TODO: Exclude, replace
                if reference.0.len() > 1 {
                    return Err(RayexecError::new(
                        "Qualified wildcard references with more than one ident not yet supported",
                    ));
                }

                let table = reference.base()?.into_normalized_string();

                let scope = bind_context
                    .iter_table_scopes(self.current)?
                    .find(|s| s.alias == table)
                    .ok_or_else(|| {
                        RayexecError::new(format!(
                            "Missing table '{table}', cannot expand wildcard"
                        ))
                    })?;

                let mut exprs = Vec::new();
                for column in &scope.column_names {
                    exprs.push(ast::SelectExpr::Expr(ast::Expr::CompoundIdent(vec![
                        ast::Ident::from_string(&table),
                        ast::Ident::from_string(column),
                    ])))
                }

                exprs
            }
            other => vec![other],
        })
    }

    pub fn bind_expression(
        &self,
        bind_context: &mut BindContext,
        expr: &ast::Expr<ResolvedMeta>,
    ) -> Result<Expression> {
        match expr {
            ast::Expr::Ident(ident) => self.bind_ident(bind_context, ident),
            _ => unimplemented!(),
        }
    }

    fn bind_ident(&self, bind_context: &mut BindContext, ident: &ast::Ident) -> Result<Expression> {
        let col = ident.as_normalized_string();

        let mut current = self.current;
        loop {
            let table = bind_context.find_table_scope_for_column(current, &col)?;
            match table {
                Some((table, col_idx)) => {
                    let table = table.reference;

                    // Table containing column found. Check if it's correlated
                    // (referencing an outer context).
                    let is_correlated = current != self.current;

                    if is_correlated {
                        // Column is correlated, Push correlation to current
                        // bind context.
                        let correlated = CorrelatedColumn {
                            outer: current,
                            table,
                            col_idx,
                        };

                        // Note `self.current`, not `current`. We want to store
                        // the context containing the expression.
                        bind_context.push_correlation(self.current, correlated)?;
                    }

                    return Ok(Expression::Column(ColumnExpr {
                        table_scope: table,
                        column: col_idx,
                    }));
                }
                None => {
                    // Table not found in current context, go to parent context
                    // relative the context we just searched.
                    match bind_context.get_parent_ref(current)? {
                        Some(parent) => current = parent,
                        None => {
                            // We're at root, no column with this ident in query.
                            return Err(RayexecError::new(format!(
                                "Missing column for reference: {col}",
                            )));
                        }
                    }
                }
            }
        }
    }
}
