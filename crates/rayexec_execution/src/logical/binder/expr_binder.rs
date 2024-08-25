use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::{
    expr::{column_expr::ColumnExpr, literal_expr::LiteralExpr, Expression},
    logical::{
        binder::bind_context::CorrelatedColumn,
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

use super::bind_context::{BindContext, BindScopeRef, TableRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecursionContext {
    pub allow_aggregate: bool,
    pub allow_window: bool,
}

#[derive(Debug)]
pub struct ExpressionBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ExpressionBinder<'a> {
    pub const fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        ExpressionBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_expression(
        &self,
        bind_context: &mut BindContext,
        expr: &ast::Expr<ResolvedMeta>,
        recur: RecursionContext,
    ) -> Result<Expression> {
        match expr {
            ast::Expr::Ident(ident) => self.bind_ident(bind_context, ident),
            ast::Expr::CompoundIdent(idents) => self.bind_idents(idents),
            ast::Expr::Literal(literal) => Self::bind_literal(literal),
            _ => unimplemented!(),
        }
    }

    pub(crate) fn bind_literal(literal: &ast::Literal<ResolvedMeta>) -> Result<Expression> {
        Ok(match literal {
            ast::Literal::Number(n) => {
                if let Ok(n) = n.parse::<i64>() {
                    Expression::Literal(LiteralExpr {
                        literal: OwnedScalarValue::Int64(n),
                    })
                } else if let Ok(n) = n.parse::<u64>() {
                    Expression::Literal(LiteralExpr {
                        literal: OwnedScalarValue::UInt64(n),
                    })
                } else if let Ok(n) = n.parse::<f64>() {
                    Expression::Literal(LiteralExpr {
                        literal: OwnedScalarValue::Float64(n),
                    })
                } else {
                    return Err(RayexecError::new(format!(
                        "Unable to parse {n} as a number"
                    )));
                }
            }
            ast::Literal::Boolean(b) => Expression::Literal(LiteralExpr {
                literal: OwnedScalarValue::Boolean(*b),
            }),
            ast::Literal::Null => Expression::Literal(LiteralExpr {
                literal: OwnedScalarValue::Null,
            }),
            ast::Literal::SingleQuotedString(s) => Expression::Literal(LiteralExpr {
                literal: OwnedScalarValue::Utf8(s.to_string().into()),
            }),
            other => {
                return Err(RayexecError::new(format!(
                    "Unusupported SQL literal: {other:?}"
                )))
            }
        })
    }

    pub(crate) fn bind_ident(
        &self,
        bind_context: &mut BindContext,
        ident: &ast::Ident,
    ) -> Result<Expression> {
        let col = ident.as_normalized_string();

        let mut current = self.current;
        loop {
            let table = bind_context.find_table_for_column(current, &col)?;
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

    /// Plan a compound identifier.
    ///
    /// Assumed to be a reference to a column either in the current scope or one
    /// of the outer scopes.
    fn bind_idents(&self, idents: &[ast::Ident]) -> Result<Expression> {
        unimplemented!()
        // fn format_err(table_ref: &TableReference, col: &str) -> String {
        //     format!("Missing column for reference: {table_ref}.{col}")
        // }

        // match idents.len() {
        //     0 => Err(RayexecError::new("Empty identifier")),
        //     1 => {
        //         // Single column.
        //         let ident = idents.pop().unwrap();
        //         self.plan_ident(ident)
        //     }
        //     2..=4 => {
        //         // Qualified column.
        //         // 2 => 'table.column'
        //         // 3 => 'schema.table.column'
        //         // 4 => 'database.schema.table.column'
        //         // TODO: Struct fields.
        //         let col = idents.pop().unwrap().into_normalized_string();
        //         let table_ref = TableReference {
        //             table: idents
        //                 .pop()
        //                 .map(|ident| ident.into_normalized_string())
        //                 .unwrap(), // Must exist
        //             schema: idents.pop().map(|ident| ident.into_normalized_string()), // May exist
        //             database: idents.pop().map(|ident| ident.into_normalized_string()), // May exist
        //         };
        //         match self.scope.resolve_column(
        //             &self.planner.outer_scopes,
        //             Some(&table_ref),
        //             &col,
        //         )? {
        //             Some(col) => Ok(LogicalExpression::ColumnRef(col)),
        //             None => Err(RayexecError::new(format_err(&table_ref, &col))), // Struct fields here.
        //         }
        //     }
        //     _ => Err(RayexecError::new(format!(
        //         "Too many identifier parts in {}",
        //         ast::ObjectReference(idents),
        //     ))), // TODO: Struct fields.
        // }
    }
}
