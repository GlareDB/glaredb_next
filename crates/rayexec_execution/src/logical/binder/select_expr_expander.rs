use crate::logical::resolver::{resolve_context::ResolveContext, ResolvedMeta};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::bind_context::{BindContext, BindContextRef};

/// Expands wildcards in expressions found in the select list.
///
/// Generates ast expressions.
#[derive(Debug)]
pub struct SelectExprExpander<'a> {
    pub current: BindContextRef,
    pub resolve_context: &'a ResolveContext,
    pub bind_context: &'a BindContext,
}

impl<'a> SelectExprExpander<'a> {
    pub fn new(
        current: BindContextRef,
        resolve_context: &'a ResolveContext,
        bind_context: &'a BindContext,
    ) -> Self {
        SelectExprExpander {
            current,
            resolve_context,
            bind_context,
        }
    }

    pub fn expand_all_select_exprs(
        &self,
        exprs: impl IntoIterator<Item = ast::SelectExpr<ResolvedMeta>>,
    ) -> Result<Vec<ast::SelectExpr<ResolvedMeta>>> {
        let mut expanded = Vec::new();
        for expr in exprs {
            let mut ex = self.expand_select_expr(expr)?;
            expanded.append(&mut ex);
        }
        Ok(expanded)
    }

    pub fn expand_select_expr(
        &self,
        expr: ast::SelectExpr<ResolvedMeta>,
    ) -> Result<Vec<ast::SelectExpr<ResolvedMeta>>> {
        Ok(match expr {
            ast::SelectExpr::Wildcard(_wildcard) => {
                // TODO: Exclude, replace
                let mut exprs = Vec::new();
                for scope in self.bind_context.iter_table_scopes(self.current)? {
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

                let scope = self
                    .bind_context
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
}
