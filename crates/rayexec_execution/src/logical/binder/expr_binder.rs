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

use super::bind_context::{BindContext, BindContextRef, TableRef};

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

    pub(crate) fn bind_aggregate(
        func: ast::Function<ResolvedMeta>,
        bind_context: &mut BindContext,
        agg_table: TableRef,
    ) -> Result<Expression> {
        unimplemented!()
    }

    pub(crate) fn bind_literal(literal: ast::Literal<ResolvedMeta>) -> Result<Expression> {
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
                literal: OwnedScalarValue::Boolean(b),
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
