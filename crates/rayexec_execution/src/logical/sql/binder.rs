use rayexec_bullet::field::DataType;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{
    ast::{self, ReplaceColumn},
    meta::{AstMeta, Raw},
    statement::{RawStatement, Statement},
};

use crate::{
    database::{catalog::CatalogTx, entry::TableEntry, DatabaseContext},
    functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction},
};

pub type BoundStatement = Statement<Bound>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound;

#[derive(Debug, Clone, PartialEq)]
pub enum BoundFunctionReference {
    Scalar(Box<dyn GenericScalarFunction>),
    Aggregate(Box<dyn GenericAggregateFunction>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundCteReference {
    /// Index into the CTE map.
    pub idx: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableOrCteReference {
    Table(TableEntry),
    Cte(BoundCteReference),
}

impl AstMeta for Bound {
    type DataSourceName = String;
    type ItemReference = String;
    type TableReference = BoundTableOrCteReference;
    type FunctionReference = BoundFunctionReference;
    type ColumnReference = String;
    type DataType = DataType;
}

#[derive(Debug)]
pub struct BindData {}

/// Binds a raw SQL AST with entries in the catalog.
#[derive(Debug)]
pub struct Binder<'a> {
    tx: &'a CatalogTx,
    context: &'a DatabaseContext,
    data: BindData,
}

impl<'a> Binder<'a> {
    pub fn new(tx: &'a CatalogTx, context: &'a DatabaseContext) -> Self {
        unimplemented!()
    }

    pub async fn bind_statement(
        mut self,
        stmt: RawStatement,
    ) -> Result<(BoundStatement, BindData)> {
        let bound = match stmt {
            Statement::Query(query) => Statement::Query(self.bind_query(query).await?),
            _ => unimplemented!(),
        };

        Ok((bound, self.data))
    }

    async fn bind_query(&mut self, query: ast::QueryNode<Raw>) -> Result<ast::QueryNode<Bound>> {
        let ctes = match query.ctes {
            Some(ctes) => Some(self.bind_ctes(ctes).await?),
            None => None,
        };

        let body = match query.body {
            ast::QueryNodeBody::Select(select) => {
                ast::QueryNodeBody::Select(Box::new(self.bind_select(*select).await?))
            }
            ast::QueryNodeBody::Values(values) => {
                ast::QueryNodeBody::Values(self.bind_values(values).await?)
            }
            ast::QueryNodeBody::Set { .. } => unimplemented!(),
        };

        // Bind ORDER BY
        let mut order_by = Vec::with_capacity(query.order_by.len());
        for expr in query.order_by {
            order_by.push(self.bind_order_by(expr).await?);
        }

        // Bind LIMIT/OFFSET
        let limit = match query.limit.limit {
            Some(expr) => Some(ExpressionBinder::new(self).bind_expression(expr).await?),
            None => None,
        };
        let offset = match query.limit.offset {
            Some(expr) => Some(ExpressionBinder::new(self).bind_expression(expr).await?),
            None => None,
        };

        Ok(ast::QueryNode {
            ctes,
            body,
            order_by,
            limit: ast::LimitModifier { limit, offset },
        })
    }

    async fn bind_ctes(
        &mut self,
        ctes: ast::CommonTableExprDefs<Raw>,
    ) -> Result<ast::CommonTableExprDefs<Bound>> {
        unimplemented!()
    }

    async fn bind_select(
        &mut self,
        select: ast::SelectNode<Raw>,
    ) -> Result<ast::SelectNode<Bound>> {
        // Bind DISTINCT
        let distinct = match select.distinct {
            Some(distinct) => Some(match distinct {
                ast::DistinctModifier::On(exprs) => {
                    let mut bound = Vec::with_capacity(exprs.len());
                    for expr in exprs {
                        bound.push(ExpressionBinder::new(self).bind_expression(expr).await?);
                    }
                    ast::DistinctModifier::On(bound)
                }
                ast::DistinctModifier::All => ast::DistinctModifier::All,
            }),
            None => None,
        };

        // Bind FROM
        let from = match select.from {
            Some(from) => Some(self.bind_from(from).await?),
            None => None,
        };

        // Bind WHERE
        let where_expr = match select.where_expr {
            Some(expr) => Some(ExpressionBinder::new(self).bind_expression(expr).await?),
            None => None,
        };

        // Bind SELECT list
        let mut projections = Vec::with_capacity(select.projections.len());
        for projection in select.projections {
            projections.push(
                ExpressionBinder::new(self)
                    .bind_select_expr(projection)
                    .await?,
            );
        }

        // Bind GROUP BY
        let group_by = match select.group_by {
            Some(group_by) => Some(match group_by {
                ast::GroupByNode::All => ast::GroupByNode::All,
                ast::GroupByNode::Exprs { exprs } => {
                    let mut bound = Vec::with_capacity(exprs.len());
                    for expr in exprs {
                        bound.push(ExpressionBinder::new(self).bind_group_by_expr(expr).await?);
                    }
                    ast::GroupByNode::Exprs { exprs: bound }
                }
            }),
            None => None,
        };

        // Bind HAVING
        let having = match select.having {
            Some(expr) => Some(ExpressionBinder::new(self).bind_expression(expr).await?),
            None => None,
        };

        Ok(ast::SelectNode {
            distinct,
            projections,
            from,
            where_expr,
            group_by,
            having,
        })
    }

    async fn bind_values(&mut self, values: ast::Values<Raw>) -> Result<ast::Values<Bound>> {
        let mut bound = Vec::with_capacity(values.rows.len());
        for row in values.rows {
            bound.push(ExpressionBinder::new(self).bind_expressions(row).await?);
        }
        Ok(ast::Values { rows: bound })
    }

    async fn bind_order_by(
        &mut self,
        order_by: ast::OrderByNode<Raw>,
    ) -> Result<ast::OrderByNode<Bound>> {
        let expr = ExpressionBinder::new(self)
            .bind_expression(order_by.expr)
            .await?;
        Ok(ast::OrderByNode {
            typ: order_by.typ,
            nulls: order_by.nulls,
            expr,
        })
    }

    async fn bind_from(&mut self, from: ast::FromNode<Raw>) -> Result<ast::FromNode<Bound>> {
        let body = match from.body {
            ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference }) => {
                if reference.0.len() != 1 {
                    return Err(RayexecError::new("Qualified table names not yet supported"));
                }
                let name = &reference.0[0].as_normalized_string();

                // TODO: If len == 1, search in CTE map.

                // TODO: Seach path.
                if let Some(entry) = self
                    .context
                    .get_catalog("temp")?
                    .get_table_entry(self.tx, "temp", name)
                    .await?
                {
                    ast::FromNodeBody::BaseTable(ast::FromBaseTable {
                        reference: BoundTableOrCteReference::Table(entry),
                    })
                } else {
                    return Err(RayexecError::new(format!(
                        "Unable to find table or view for '{name}'"
                    )));
                }
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                ast::FromNodeBody::Subquery(ast::FromSubquery {
                    query: Box::pin(self.bind_query(query)).await?,
                })
            }
            ast::FromNodeBody::TableFunction(ast::FromTableFunction { .. }) => {
                unimplemented!()
            }
            ast::FromNodeBody::Join(ast::FromJoin {
                left,
                right,
                join_type,
                join_condition,
            }) => {
                let left = Box::pin(self.bind_from(*left)).await?;
                let right = Box::pin(self.bind_from(*right)).await?;

                let join_condition = match join_condition {
                    ast::JoinCondition::On(expr) => {
                        let expr = ExpressionBinder::new(self).bind_expression(expr).await?;
                        ast::JoinCondition::On(expr)
                    }
                    ast::JoinCondition::Using(idents) => ast::JoinCondition::Using(idents),
                    ast::JoinCondition::Natural => ast::JoinCondition::Natural,
                    ast::JoinCondition::None => ast::JoinCondition::None,
                };

                ast::FromNodeBody::Join(ast::FromJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                    join_condition,
                })
            }
        };

        Ok(ast::FromNode {
            alias: from.alias,
            body,
        })
    }
}

struct ExpressionBinder<'a> {
    binder: &'a Binder<'a>,
}

impl<'a> ExpressionBinder<'a> {
    fn new(binder: &'a Binder) -> Self {
        ExpressionBinder { binder }
    }

    async fn bind_select_expr(
        &self,
        select_expr: ast::SelectExpr<Raw>,
    ) -> Result<ast::SelectExpr<Bound>> {
        match select_expr {
            ast::SelectExpr::Expr(expr) => {
                Ok(ast::SelectExpr::Expr(self.bind_expression(expr).await?))
            }
            ast::SelectExpr::AliasedExpr(expr, alias) => Ok(ast::SelectExpr::AliasedExpr(
                self.bind_expression(expr).await?,
                alias,
            )),
            ast::SelectExpr::QualifiedWildcard(object_name, wildcard) => {
                Ok(ast::SelectExpr::QualifiedWildcard(
                    object_name,
                    self.bind_wildcard(wildcard).await?,
                ))
            }
            ast::SelectExpr::Wildcard(wildcard) => Ok(ast::SelectExpr::Wildcard(
                self.bind_wildcard(wildcard).await?,
            )),
        }
    }

    async fn bind_wildcard(&self, wildcard: ast::Wildcard<Raw>) -> Result<ast::Wildcard<Bound>> {
        let mut replace_cols = Vec::with_capacity(wildcard.replace_cols.len());
        for replace in wildcard.replace_cols {
            replace_cols.push(ReplaceColumn {
                col: replace.col,
                expr: self.bind_expression(replace.expr).await?,
            });
        }

        Ok(ast::Wildcard {
            exclude_cols: wildcard.exclude_cols,
            replace_cols,
        })
    }

    async fn bind_group_by_expr(
        &self,
        expr: ast::GroupByExpr<Raw>,
    ) -> Result<ast::GroupByExpr<Bound>> {
        Ok(match expr {
            ast::GroupByExpr::Expr(exprs) => {
                ast::GroupByExpr::Expr(self.bind_expressions(exprs).await?)
            }
            ast::GroupByExpr::Cube(exprs) => {
                ast::GroupByExpr::Cube(self.bind_expressions(exprs).await?)
            }
            ast::GroupByExpr::Rollup(exprs) => {
                ast::GroupByExpr::Rollup(self.bind_expressions(exprs).await?)
            }
            ast::GroupByExpr::GroupingSets(exprs) => {
                ast::GroupByExpr::GroupingSets(self.bind_expressions(exprs).await?)
            }
        })
    }

    async fn bind_expressions(
        &self,
        exprs: impl IntoIterator<Item = ast::Expr<Raw>>,
    ) -> Result<Vec<ast::Expr<Bound>>> {
        let mut bound = Vec::new();
        for expr in exprs {
            bound.push(self.bind_expression(expr).await?);
        }
        Ok(bound)
    }

    /// Bind an expression.
    async fn bind_expression(&self, expr: ast::Expr<Raw>) -> Result<ast::Expr<Bound>> {
        match expr {
            ast::Expr::Function(func) => {
                // TODO: Search path (with system being the first to check)
                if func.reference.0.len() != 1 {
                    return Err(RayexecError::new(
                        "Qualified function names not yet supported",
                    ));
                }
                let func_name = &func.reference.0[0].as_normalized_string();
                let catalog = "system";
                let schema = "glare_catalog";

                let filter = match func.filter {
                    Some(filter) => Some(Box::new(Box::pin(self.bind_expression(*filter)).await?)),
                    None => None,
                };

                let mut args = Vec::with_capacity(func.args.len());
                for func_arg in func.args {
                    let func_arg = match func_arg {
                        ast::FunctionArg::Named { name, arg } => ast::FunctionArg::Named {
                            name,
                            arg: Box::pin(self.bind_expression(arg)).await?,
                        },
                        ast::FunctionArg::Unnamed { arg } => ast::FunctionArg::Unnamed {
                            arg: Box::pin(self.bind_expression(arg)).await?,
                        },
                    };
                    args.push(func_arg);
                }

                // Check scalars first.
                if let Some(scalar) = self
                    .binder
                    .context
                    .get_catalog(catalog)?
                    .get_scalar_fn(self.binder.tx, schema, func_name)
                    .await?
                {
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: BoundFunctionReference::Scalar(scalar),
                        args,
                        filter,
                    }));
                }

                // Now check aggregates.
                if let Some(aggregate) = self
                    .binder
                    .context
                    .get_catalog(catalog)?
                    .get_aggregate_fn(self.binder.tx, schema, func_name)
                    .await?
                {
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: BoundFunctionReference::Aggregate(aggregate),
                        args,
                        filter,
                    }));
                }

                Err(RayexecError::new(format!(
                    "Cannot resolve function with name {}",
                    func.reference
                )))
            }
            _ => unimplemented!(),
        }
    }
}
