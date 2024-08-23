use crate::logical::resolver::ResolvedMeta;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;
use std::collections::HashMap;

#[derive(Debug)]
pub struct SelectList {
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
