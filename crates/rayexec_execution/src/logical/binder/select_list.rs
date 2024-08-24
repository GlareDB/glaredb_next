use crate::{
    expr::Expression,
    logical::{
        binder::expr_binder::ExpressionBinder,
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;
use std::collections::HashMap;

use super::bind_context::{BindContext, BindContextRef, TableScopeRef};

#[derive(Debug)]
pub struct SelectList {
    /// The table scope that expressions referencing columns in the select list
    /// should bind to.
    pub table: TableScopeRef,

    /// Mapping from explicit user-provided alias to column index in the output.
    pub alias_map: HashMap<String, usize>,

    /// Expanded projections that will be shown in the output.
    pub projections: Vec<ast::SelectExpr<ResolvedMeta>>,

    /// Projections that are appended to the right of the output projects.
    ///
    /// This is for appending expressions used for ORDER BY and GROUP BY.
    pub appended: Vec<ast::SelectExpr<ResolvedMeta>>,
}

impl SelectList {
    /// Binds the select expressions in this to the provided bind context.
    pub fn bind_expressions(
        &self,
        bind_ref: BindContextRef,
        bind_context: &mut BindContext,
        resolve_context: &ResolveContext,
    ) -> Result<Vec<Expression>> {
        let expr_binder = ExpressionBinder::new(bind_ref, resolve_context);

        let mut ast_exprs = Vec::with_capacity(self.projections.len() + self.appended.len());
        for expr in self.projections.iter().chain(&self.appended) {
            let expr = match expr {
                ast::SelectExpr::Expr(expr) => expr,
                ast::SelectExpr::AliasedExpr(expr, _) => expr,
                ast::SelectExpr::QualifiedWildcard(_, _) | ast::SelectExpr::Wildcard(_) => {
                    return Err(RayexecError::new(
                        "Encountered unexpanded wildcard in select list",
                    ))
                }
            };
            ast_exprs.push(expr);
        }

        // Bind both user projections and appended projections.
        let mut expressions = Vec::with_capacity(self.projections.len());
        for &expr in ast_exprs.iter() {
            let bound = expr_binder.bind_expression(bind_context, expr)?;
            expressions.push(bound);
        }

        // Generate projection names. Appended columns receive generated names.
        let mut names: Vec<_> = (0..self.projections.len())
            .map(|_| String::new())
            .chain((0..self.appended.len()).map(|idx| format!("__generated_{idx}")))
            .collect();

        // Init with user provided aliases.
        for (alias, idx) in &self.alias_map {
            names[*idx] = alias.clone();
        }

        // Generate names from user expressions.
        for (name, expr) in names.iter_mut().zip(&ast_exprs) {
            if !name.is_empty() {
                continue;
            }

            match expr {
                ast::Expr::Ident(ident) => *name = ident.as_normalized_string(),
                ast::Expr::CompoundIdent(idents) => {
                    *name = idents
                        .last()
                        .map(|i| i.as_normalized_string())
                        .unwrap_or_else(|| "?column?".to_string())
                }
                ast::Expr::Function(ast::Function { reference, .. }) => {
                    let (func, _) = resolve_context.functions.try_get_bound(*reference)?;
                    *name = func.name().to_string();
                }
                _ => *name = "?column?".to_string(),
            }
        }

        let types = expressions
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        debug_assert_eq!(names.len(), types.len());

        let table = bind_context.get_table_mut(self.table)?;
        // TODO: Probably assert these are still empty before writing over them.
        table.column_names = names;
        table.column_types = types;

        Ok(expressions)
    }

    /// Appends an expression to the select list for later binding.
    ///
    /// Logically this places the expression to the right of existing columns.
    // TODO: Can this allow aliases?
    pub fn append_expression(&mut self, expr: ast::SelectExpr<ResolvedMeta>) -> usize {
        let idx = self.projections.len() + self.appended.len();
        self.appended.push(expr);
        idx
    }

    /// Attempt to get an expression with the possibility of it pointing to an
    /// expression in the select list.
    ///
    /// This allows GROUP BY and ORDER BY to reference columns in the output by
    /// either its alias, or by its ordinal.
    pub fn get_projection_reference(
        &self,
        expr: &ast::Expr<ResolvedMeta>,
    ) -> Result<Option<usize>> {
        // Check constant first.
        //
        // e.g. ORDER BY 1
        if let ast::Expr::Literal(ast::Literal::Number(s)) = expr {
            let n = s
                .parse::<i64>()
                .map_err(|_| RayexecError::new(format!("Failed to parse '{s}' into a number")))?;
            if n < 1 || n as usize > self.projections.len() {
                return Err(RayexecError::new(format!(
                    "Column out of range, expected 1 - {}",
                    self.projections.len()
                )))?;
            }

            return Ok(Some(n as usize));
        }

        // Alias reference
        if let ast::Expr::Ident(ident) = expr {
            if let Some(idx) = self.alias_map.get(&ident.as_normalized_string()) {
                return Ok(Some(*idx));
            }
        }

        Ok(None)
    }
}
