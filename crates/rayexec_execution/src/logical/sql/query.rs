use std::collections::HashMap;

use super::{
    aggregate::AggregatePlanner,
    binder::{BindData, Bound, BoundCteReference, BoundTableOrCteReference},
    expr::{ExpandedSelectExpr, ExpressionContext},
    planner::LogicalQuery,
    scope::{ColumnRef, Scope, TableReference},
    subquery::SubqueryPlanner,
};
use crate::logical::expr::LogicalExpression;
use crate::logical::operator::{
    AnyJoin, CrossJoin, ExpressionList, Filter, JoinType, Limit, LogicalOperator, Order,
    OrderByExpr, Projection, Scan, TableFunction,
};
use rayexec_bullet::field::TypeSchema;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast::{self, OrderByNulls, OrderByType};

const EMPTY_SCOPE: &Scope = &Scope::empty();
const EMPTY_TYPE_SCHEMA: &TypeSchema = &TypeSchema::empty();

/// Plans query nodes.
#[derive(Debug, Clone)]
pub struct QueryNodePlanner<'a> {
    /// Outer scopes relative to the query we're currently planning.
    pub outer_scopes: Vec<Scope>,
    /// Data collected during binding (table references, functions, etc).
    pub bind_data: &'a BindData,
}

impl<'a> QueryNodePlanner<'a> {
    pub fn new(bind_data: &'a BindData) -> Self {
        QueryNodePlanner {
            outer_scopes: Vec::new(),
            bind_data,
        }
    }

    /// Create a new nested plan context for planning subqueries.
    pub fn nested(&self, outer: Scope) -> Self {
        QueryNodePlanner {
            outer_scopes: std::iter::once(outer)
                .chain(self.outer_scopes.clone())
                .collect(),
            bind_data: self.bind_data,
        }
    }

    pub fn plan_query(&mut self, query: ast::QueryNode<Bound>) -> Result<LogicalQuery> {
        let mut planned = match query.body {
            ast::QueryNodeBody::Select(select) => self.plan_select(*select, query.order_by)?,
            ast::QueryNodeBody::Set {
                left: _,
                right: _,
                operation: _,
            } => unimplemented!(),
            ast::QueryNodeBody::Values(values) => self.plan_values(values)?,
        };

        // Handle LIMIT/OFFSET
        let expr_ctx = ExpressionContext::new(self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
        if let Some(limit_expr) = query.limit.limit {
            let expr = expr_ctx.plan_expression(limit_expr)?;
            let limit = expr.try_into_scalar()?.try_as_i64()? as usize;

            let offset = match query.limit.offset {
                Some(offset_expr) => {
                    let expr = expr_ctx.plan_expression(offset_expr)?;
                    let offset = expr.try_into_scalar()?.try_as_i64()?;
                    Some(offset as usize)
                }
                None => None,
            };

            // Update plan, does not change scope.
            planned.root = LogicalOperator::Limit(Limit {
                offset,
                limit,
                input: Box::new(planned.root),
            });
        }

        Ok(planned)
    }

    fn plan_select(
        &mut self,
        select: ast::SelectNode<Bound>,
        order_by: Vec<ast::OrderByNode<Bound>>,
    ) -> Result<LogicalQuery> {
        // Handle FROM
        let mut plan = match select.from {
            Some(from) => self.plan_from_node(from, Scope::empty())?,
            None => LogicalQuery {
                root: LogicalOperator::Empty,
                scope: Scope::empty(),
            },
        };

        let from_type_schema = plan.root.output_schema(&[])?;
        let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);

        // Handle WHERE
        if let Some(where_expr) = select.where_expr {
            let mut expr = expr_ctx.plan_expression(where_expr)?;
            SubqueryPlanner.plan_subquery_expr(&mut expr, &mut plan.root)?;

            // Add filter to the plan, does not change the scope.
            plan.root = LogicalOperator::Filter(Filter {
                predicate: expr,
                input: Box::new(plan.root),
            });
        }

        // Expand SELECT.
        let projections = expr_ctx.expand_all_select_exprs(select.projections)?;
        if projections.is_empty() {
            return Err(RayexecError::new("Cannot SELECT * without a FROM clause"));
        }

        // Add projections to plan using previously expanded select items.
        let mut select_exprs = Vec::with_capacity(projections.len());
        let mut names = Vec::with_capacity(projections.len());
        let mut alias_map = HashMap::new();
        for (idx, proj) in projections.into_iter().enumerate() {
            match proj {
                ExpandedSelectExpr::Expr {
                    expr,
                    name,
                    explicit_alias,
                } => {
                    if explicit_alias {
                        alias_map.insert(name.clone(), idx);
                    }
                    let expr = expr_ctx.plan_expression(expr)?;
                    select_exprs.push(expr);
                    names.push(name);
                }
                ExpandedSelectExpr::Column { idx, name } => {
                    let expr = LogicalExpression::ColumnRef(ColumnRef {
                        scope_level: 0,
                        item_idx: idx,
                    });
                    select_exprs.push(expr);
                    names.push(name);
                }
            }
        }

        // Plan and append HAVING and ORDER BY expressions.
        //
        // This may result in new expressions that need to be added to the
        // select expressions. However, this should not modify the final query
        // projection.
        let mut num_appended = 0;
        let having_expr = match select.having {
            Some(expr) => {
                let mut expr = expr_ctx.plan_expression(expr)?;
                num_appended += Self::append_hidden(&mut select_exprs, &mut expr)?;
                Some(expr)
            }
            None => None,
        };

        let mut order_by_exprs = order_by
            .into_iter()
            .map(|order_by| {
                let expr = expr_ctx.plan_expression_with_select_list(
                    &alias_map,
                    &select_exprs,
                    order_by.expr,
                )?;
                Ok(OrderByExpr {
                    expr,
                    desc: matches!(order_by.typ.unwrap_or(OrderByType::Asc), OrderByType::Desc),
                    nulls_first: matches!(
                        order_by.nulls.unwrap_or(OrderByNulls::First),
                        OrderByNulls::First
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        num_appended += Self::append_order_by_exprs(&mut select_exprs, &mut order_by_exprs)?;

        // GROUP BY

        // Group by has access to everything we've planned so far.
        let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);
        plan.root = AggregatePlanner.plan(
            expr_ctx,
            &mut select_exprs,
            &alias_map,
            plan.root,
            select.group_by,
        )?;

        // Project the full select list.
        plan = LogicalQuery {
            root: LogicalOperator::Projection(Projection {
                exprs: select_exprs.clone(),
                input: Box::new(plan.root),
            }),
            scope: plan.scope,
        };

        // Add filter for HAVING.
        if let Some(expr) = having_expr {
            plan = LogicalQuery {
                root: LogicalOperator::Filter(Filter {
                    predicate: expr,
                    input: Box::new(plan.root),
                }),
                scope: plan.scope,
            }
        }

        // Add order by node.
        if !order_by_exprs.is_empty() {
            plan = LogicalQuery {
                root: LogicalOperator::Order(Order {
                    exprs: order_by_exprs,
                    input: Box::new(plan.root),
                }),
                scope: plan.scope,
            }
        }

        // Turn select expressions back into only the expressions for the
        // output.
        if num_appended > 0 {
            let output_len = select_exprs.len() - num_appended;

            let projections = (0..output_len).map(LogicalExpression::new_column).collect();

            plan = LogicalQuery {
                root: LogicalOperator::Projection(Projection {
                    exprs: projections,
                    input: Box::new(plan.root),
                }),
                scope: plan.scope,
            };
        }

        // Flatten subqueries;
        plan.root = SubqueryPlanner.flatten(plan.root)?;

        // Cleaned scope containing only output columns in the final output.
        plan.scope = Scope::with_columns(None, names);

        Ok(plan)
    }

    pub fn plan_from_node(
        &self,
        from: ast::FromNode<Bound>,
        current_scope: Scope,
    ) -> Result<LogicalQuery> {
        // Plan the "body" of the FROM.
        let body = match from.body {
            ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference }) => {
                match reference {
                    BoundTableOrCteReference::Table {
                        catalog,
                        schema,
                        entry,
                    } => {
                        // Scope reference for base tables is always fully
                        // qualified. This query is valid:
                        //
                        // SELECT my_catalog.my_schema.t1.a FROM t1
                        let scope_reference = TableReference {
                            database: Some(catalog.clone()),
                            schema: Some(schema.clone()),
                            table: entry.name.clone(),
                        };
                        let scope = Scope::with_columns(
                            Some(scope_reference),
                            entry.columns.iter().map(|f| f.name.clone()),
                        );
                        LogicalQuery {
                            root: LogicalOperator::Scan(Scan {
                                catalog,
                                schema,
                                source: entry,
                            }),
                            scope,
                        }
                    }
                    BoundTableOrCteReference::Cte(BoundCteReference { idx }) => {
                        let cte = self.bind_data.ctes.get(idx).ok_or_else(|| {
                            RayexecError::new(format!("Missing bound CTE at index {idx}"))
                        })?;

                        if cte.materialized {
                            // Will probably just be a variant of our recursive
                            // CTE support with a "materialized" operator.
                            return Err(RayexecError::new(
                                "Materialized CTEs not currently supported",
                            ));
                        }

                        let scope_reference = TableReference {
                            database: None,
                            schema: None,
                            table: cte.name.clone(),
                        };

                        // TODO: Unsure how we want to set the scope for recursive yet.

                        // Plan CTE body...
                        let mut nested = self.nested(current_scope);
                        let mut query = nested.plan_query(cte.body.clone())?;

                        // Update resulting scope items with new cte reference.
                        query
                            .scope
                            .iter_mut()
                            .for_each(|item| item.alias = Some(scope_reference.clone()));

                        // Apply user provided aliases.
                        if let Some(aliases) = cte.column_aliases.as_ref() {
                            if aliases.len() > query.scope.items.len() {
                                return Err(RayexecError::new(format!(
                                    "Expected at most {} column aliases, received {}",
                                    query.scope.items.len(),
                                    aliases.len()
                                )))?;
                            }

                            for (item, alias) in query.scope.iter_mut().zip(aliases.iter()) {
                                item.column = alias.as_normalized_string();
                            }
                        }

                        // And return it. It's now usable elsewhere in the plan.
                        query
                    }
                }
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                let mut nested = self.nested(current_scope);
                nested.plan_query(query)?
            }
            ast::FromNodeBody::TableFunction(ast::FromTableFunction { reference, args: _ }) => {
                let scope_reference = TableReference {
                    database: None,
                    schema: None,
                    table: reference.name,
                };
                let scope = Scope::with_columns(
                    Some(scope_reference),
                    reference.func.schema().fields.into_iter().map(|f| f.name),
                );

                let operator = LogicalOperator::TableFunction(TableFunction {
                    function: reference.func,
                });

                LogicalQuery {
                    root: operator,
                    scope,
                }
            }
            ast::FromNodeBody::Join(ast::FromJoin {
                left,
                right,
                join_type,
                join_condition,
            }) => {
                // Plan left side of join.
                let left_nested = self.nested(current_scope.clone());
                let left_plan = left_nested.plan_from_node(*left, Scope::empty())?; // TODO: Determine if should be empty.

                // Plan right side of join.
                //
                // Note this uses a plan context that has the "left" scope as
                // its outer scope.
                let right_nested = left_nested.nested(left_plan.scope.clone());
                let right_plan = right_nested.plan_from_node(*right, Scope::empty())?; // TODO: Determine if this should be empty.

                match join_condition {
                    ast::JoinCondition::On(on) => {
                        let merged = left_plan.scope.merge(right_plan.scope)?;
                        let left_schema = left_plan.root.output_schema(&[])?; // TODO: Outers
                        let right_schema = right_plan.root.output_schema(&[])?; // TODO: Outers
                        let merged_schema = left_schema.merge(right_schema);
                        let expr_ctx =
                            ExpressionContext::new(&left_nested, &merged, &merged_schema);

                        let on_expr = expr_ctx.plan_expression(on)?;

                        let join_type = match join_type {
                            ast::JoinType::Inner => JoinType::Inner,
                            ast::JoinType::Left => JoinType::Left,
                            ast::JoinType::Right => JoinType::Right,
                            ast::JoinType::Cross => {
                                unreachable!("Cross join should not have a join condition")
                            }
                            _ => unimplemented!(),
                        };

                        LogicalQuery {
                            root: LogicalOperator::AnyJoin(AnyJoin {
                                left: Box::new(left_plan.root),
                                right: Box::new(right_plan.root),
                                join_type,
                                on: on_expr,
                            }),
                            scope: merged,
                        }
                    }
                    ast::JoinCondition::None => match join_type {
                        ast::JoinType::Cross => {
                            let merged = left_plan.scope.merge(right_plan.scope)?;
                            LogicalQuery {
                                root: LogicalOperator::CrossJoin(CrossJoin {
                                    left: Box::new(left_plan.root),
                                    right: Box::new(right_plan.root),
                                }),
                                scope: merged,
                            }
                        }
                        _other => return Err(RayexecError::new("Missing join condition for join")),
                    },
                    _ => unimplemented!(),
                }
            }
            ast::FromNodeBody::File(_) => {
                return Err(RayexecError::new(
                    "Binder should have replace file path with a table function",
                ))
            }
        };

        // Apply aliases if provided.
        let aliased_scope = Self::apply_alias(body.scope, from.alias)?;

        Ok(LogicalQuery {
            root: body.root,
            scope: aliased_scope,
        })
    }

    /// Apply table and column aliases to a scope.
    fn apply_alias(mut scope: Scope, alias: Option<ast::FromAlias>) -> Result<Scope> {
        Ok(match alias {
            Some(ast::FromAlias { alias, columns }) => {
                let reference = TableReference {
                    database: None,
                    schema: None,
                    table: alias.into_normalized_string(),
                };

                // Modify all items in the scope to now have the new table
                // alias.
                for item in scope.items.iter_mut() {
                    // TODO: Make sure that it's correct to apply this to
                    // everything in the scope.
                    item.alias = Some(reference.clone());
                }

                // If column aliases are provided as well, apply those to the
                // columns in the scope.
                //
                // Note that if the user supplies less aliases than there are
                // columns in the scope, then the remaining columns will retain
                // their original names.
                if let Some(columns) = columns {
                    if columns.len() > scope.items.len() {
                        return Err(RayexecError::new(format!(
                            "Specified {} column aliases when only {} columns exist",
                            columns.len(),
                            scope.items.len()
                        )));
                    }

                    for (item, new_alias) in scope.items.iter_mut().zip(columns.into_iter()) {
                        item.column = new_alias.into_normalized_string();
                    }
                }

                scope
            }
            None => scope,
        })
    }

    fn plan_values(&self, values: ast::Values<Bound>) -> Result<LogicalQuery> {
        if values.rows.is_empty() {
            return Err(RayexecError::new("Empty VALUES expression"));
        }

        // Convert AST expressions to logical expressions.
        let expr_ctx = ExpressionContext::new(self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
        let num_cols = values.rows[0].len();
        let exprs = values
            .rows
            .into_iter()
            .map(|col_vals| {
                col_vals
                    .into_iter()
                    .map(|col_expr| expr_ctx.plan_expression(col_expr))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<Vec<LogicalExpression>>>>()?;

        let operator = LogicalOperator::ExpressionList(ExpressionList { rows: exprs });

        // Generate output scope with appropriate column names.
        let mut scope = Scope::empty();
        scope.add_columns(None, (0..num_cols).map(|i| format!("column{}", i + 1)));

        Ok(LogicalQuery {
            root: operator,
            scope,
        })
    }

    /// Appends new expressions that need to be added to the plan for order by
    /// expressions. The order by expression will be rewritten to point to the
    /// new expression, or to an expression already in the select exprs.
    fn append_order_by_exprs(
        select_exprs: &mut Vec<LogicalExpression>,
        order_by_exprs: &mut [OrderByExpr],
    ) -> Result<usize> {
        let mut num_appended = 0;
        for order_by_expr in order_by_exprs.iter_mut() {
            num_appended += Self::append_hidden(select_exprs, &mut order_by_expr.expr)?;
        }

        Ok(num_appended)
    }

    /// Appends hidden columns (columns that aren't referenced in the outer
    /// plan's scope) to the select expressions if the provided expression is
    /// not already included in the select expressions.
    ///
    /// This is needed for the pre-projection into ORDER BY and HAVING, where
    /// the expressions in those clauses may not actually exist in the output.
    ///
    /// Returns the number of appended expressions.
    fn append_hidden(
        select_exprs: &mut Vec<LogicalExpression>,
        expr: &mut LogicalExpression,
    ) -> Result<usize> {
        // Check to see if our expression matches anything already in the select
        // list. If it does, replace our expression with a reference to it.
        for (select_idx, select_expr) in select_exprs.iter().enumerate() {
            if expr == select_expr {
                *expr = LogicalExpression::new_column(select_idx);
                return Ok(0);
            }
        }

        // TODO: Check if `expr` is already a column ref pointing to a select
        // expr.

        // Otherwise need to put the expression into the select list, and
        // replace it with a reference.
        let col_ref = LogicalExpression::new_column(select_exprs.len());
        let orig = std::mem::replace(expr, col_ref);

        select_exprs.push(orig);

        Ok(1)
    }
}
