use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::expr::{column_expr::ColumnExpr, Expression};

use super::bind_context::{BindContext, BindScopeRef, CorrelatedColumn, TableAlias};

/// Defined behavior of how to bind idents to actual columns.
///
/// Implementations should typically implement custom logic first, then fall
/// back to the default binder.
// TODO: Literal binder.
pub trait ExpressionColumnBinder {
    fn bind_from_ident(
        &mut self,
        bind_context: &mut BindContext,
        ident: &ast::Ident,
    ) -> Result<Expression>;

    fn bind_from_idents(
        &mut self,
        bind_context: &mut BindContext,
        idents: &[ast::Ident],
    ) -> Result<Expression>;
}

/// Default column binder.
///
/// Attempts to bind to columns in the current scope, working its way up to
/// outer scopes if the current scope does not contain a suitable columns.
///
/// Binding to an outer scope will push a correlation for the current scope.
#[derive(Debug, Clone, Copy)]
pub struct DefaultColumnBinder {
    pub current: BindScopeRef,
}

impl ExpressionColumnBinder for DefaultColumnBinder {
    fn bind_from_ident(
        &mut self,
        bind_context: &mut BindContext,
        ident: &ast::Ident,
    ) -> Result<Expression> {
        let col = ident.as_normalized_string();
        self.bind_column(bind_context, None, &col)
    }

    fn bind_from_idents(
        &mut self,
        bind_context: &mut BindContext,
        idents: &[ast::Ident],
    ) -> Result<Expression> {
        let (alias, col) = idents_to_alias_and_column(idents)?;
        self.bind_column(bind_context, alias, &col)
    }
}

impl DefaultColumnBinder {
    pub fn new(current: BindScopeRef) -> Self {
        DefaultColumnBinder { current }
    }

    fn bind_column(
        &self,
        bind_context: &mut BindContext,
        alias: Option<TableAlias>,
        col: &str,
    ) -> Result<Expression> {
        let mut current = self.current;
        loop {
            let table = bind_context.find_table_for_column(current, alias.as_ref(), &col)?;
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

/// Try to convert idents into a table alias and column pair.
///
/// If only one ident is provided, table alias will be None.
///
/// Errors if no idents are provided.
pub fn idents_to_alias_and_column(idents: &[ast::Ident]) -> Result<(Option<TableAlias>, String)> {
    match idents.len() {
        0 => Err(RayexecError::new("Empty identifier")),
        1 => {
            // Single column.
            Ok((None, idents[0].as_normalized_string()))
        }
        2..=4 => {
            // Qualified column.
            // 2 => 'table.column'
            // 3 => 'schema.table.column'
            // 4 => 'database.schema.table.column'
            // TODO: Struct fields.

            let mut idents = idents.to_vec();
            let col = idents.pop().unwrap().into_normalized_string();

            let alias = TableAlias {
                table: idents
                    .pop()
                    .map(|ident| ident.into_normalized_string())
                    .unwrap(), // Must exist
                schema: idents.pop().map(|ident| ident.into_normalized_string()), // May exist
                database: idents.pop().map(|ident| ident.into_normalized_string()), // May exist
            };

            Ok((Some(alias), col))
        }
        _ => Err(RayexecError::new(format!(
            "Too many identifier parts in {}",
            ast::ObjectReference(idents.to_vec()),
        ))), // TODO: Struct fields.
    }
}

/// Column binder that errors on any attempt to bind to a column.
#[derive(Debug, Clone, Copy)]
pub struct ErroringColumnBinder;

impl ExpressionColumnBinder for ErroringColumnBinder {
    fn bind_from_ident(
        &mut self,
        _bind_context: &mut BindContext,
        _ident: &ast::Ident,
    ) -> Result<Expression> {
        Err(RayexecError::new(
            "Statement does not support binding to columns",
        ))
    }

    fn bind_from_idents(
        &mut self,
        _bind_context: &mut BindContext,
        _idents: &[ast::Ident],
    ) -> Result<Expression> {
        Err(RayexecError::new(
            "Statement does not support binding to columns",
        ))
    }
}