use std::collections::HashMap;

use crate::logical::{
    context::QueryContext,
    expr::LogicalExpression,
    operator::{EqualityJoin, Node, SetOpKind, SetOperation},
    resolver::{
        resolve_context::ResolveContext,
        resolved_table::{CteIndex, ResolvedTableOrCteReference, ResolvedTableReference},
        ResolvedMeta,
    },
};
use crate::{
    functions::implicit::implicit_cast_score,
    logical::operator::{
        AnyJoin, CrossJoin, ExpressionList, Filter, JoinType, Limit, LogicalOperator, Order,
        OrderByExpr, Projection, Scan, TableFunction,
    },
};
use rayexec_bullet::field::TypeSchema;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast::{self, OrderByNulls, OrderByType};

use super::{
    plan_aggregate2::AggregatePlanner,
    plan_expr::{ExpandedSelectExpr, ExpressionContext},
    plan_statement2::LogicalQuery2,
    plan_subquery2::SubqueryPlanner,
    scope::{ColumnRef, Scope, TableReference},
};

const EMPTY_SCOPE: &Scope = &Scope::empty();
const EMPTY_TYPE_SCHEMA: &TypeSchema = &TypeSchema::empty();

/// Plans query nodes.
#[derive(Debug, Clone)]
pub struct QueryNodePlanner<'a> {
    /// Outer schemas relative to the query we're currently planning.
    pub outer_schemas: Vec<TypeSchema>,

    /// Outer scopes relative to the query we're currently planning.
    pub outer_scopes: Vec<Scope>,

    /// Data collected during binding (table references, functions, etc).
    pub bind_data: &'a ResolveContext,
}

impl<'a> QueryNodePlanner<'a> {
    pub fn new(bind_data: &'a ResolveContext) -> Self {
        QueryNodePlanner {
            outer_schemas: Vec::new(),
            outer_scopes: Vec::new(),
            bind_data,
        }
    }

    /// Create a new nested plan context for planning subqueries.
    pub fn nested(&self, outer_schema: TypeSchema, outer_scope: Scope) -> Self {
        QueryNodePlanner {
            outer_schemas: std::iter::once(outer_schema)
                .chain(self.outer_schemas.clone())
                .collect(),
            outer_scopes: std::iter::once(outer_scope)
                .chain(self.outer_scopes.clone())
                .collect(),
            bind_data: self.bind_data,
        }
    }

    pub fn plan_query(
        &mut self,
        context: &mut QueryContext,
        query: ast::QueryNode<ResolvedMeta>,
    ) -> Result<LogicalQuery2> {
        let mut planned = self.plan_query_body(context, query.body, query.order_by)?;

        // Handle LIMIT/OFFSET
        let expr_ctx = ExpressionContext::new(self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
        if let Some(limit_expr) = query.limit.limit {
            let expr = expr_ctx.plan_expression(context, limit_expr)?;
            let limit = expr.try_into_scalar()?.try_as_i64()? as usize;

            let offset = match query.limit.offset {
                Some(offset_expr) => {
                    let expr = expr_ctx.plan_expression(context, offset_expr)?;
                    let offset = expr.try_into_scalar()?.try_as_i64()?;
                    Some(offset as usize)
                }
                None => None,
            };

            // Update plan, does not change scope.
            planned.root = LogicalOperator::Limit2(Node::new(Limit {
                offset,
                limit,
                input: Box::new(planned.root),
            }));
        }

        Ok(planned)
    }

    fn plan_query_body(
        &mut self,
        context: &mut QueryContext,
        body: ast::QueryNodeBody<ResolvedMeta>,
        order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
    ) -> Result<LogicalQuery2> {
        Ok(match body {
            ast::QueryNodeBody::Select(select) => self.plan_select(context, *select, order_by)?,
            ast::QueryNodeBody::Nested(nested) => self.plan_query(context, *nested)?,
            ast::QueryNodeBody::Set {
                left,
                right,
                operation,
                all,
            } => {
                let top = self.plan_query_body(context, *left, None)?;
                let bottom = self.plan_query_body(context, *right, None)?;

                // Output scope always takes the scope from the top. Aliases and
                // column names from the bottom get thrown away.
                let scope = top.scope;

                let [top, bottom] =
                    Self::apply_setop_casts(top.root, bottom.root, &self.outer_schemas)?;

                let kind = match operation {
                    ast::SetOperation::Union => SetOpKind::Union,
                    ast::SetOperation::Except => SetOpKind::Except,
                    ast::SetOperation::Intersect => SetOpKind::Intersect,
                };

                let plan = LogicalOperator::SetOperation(Node::new(SetOperation {
                    top: Box::new(top),
                    bottom: Box::new(bottom),
                    kind,
                    all,
                }));

                // TODO: Apply ORDER BY to plan making use of scope. Similar to
                // what happens in planning select.
                if order_by.is_some() {
                    not_implemented!("order by on set ops");
                }

                LogicalQuery2 { root: plan, scope }
            }
            ast::QueryNodeBody::Values(values) => self.plan_values(context, values)?,
        })
    }

    fn plan_select(
        &mut self,
        context: &mut QueryContext,
        select: ast::SelectNode<ResolvedMeta>,
        order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
    ) -> Result<LogicalQuery2> {
        // Handle FROM
        let mut plan = match select.from {
            Some(from) => {
                self.plan_from_node(context, from, TypeSchema::empty(), Scope::empty())?
            }
            None => LogicalQuery2 {
                root: LogicalOperator::EMPTY,
                scope: Scope::empty(),
            },
        };

        let from_type_schema = plan.root.output_schema(&[])?;
        let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);

        // Handle WHERE
        if let Some(where_expr) = select.where_expr {
            let expr = expr_ctx.plan_expression(context, where_expr)?;
            // SubqueryPlanner.plan_subquery_expr(&mut expr, &mut plan.root)?;

            // Add filter to the plan, does not change the scope.
            plan.root = LogicalOperator::Filter2(Node::new(Filter {
                predicate: expr,
                input: Box::new(plan.root),
            }));
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
                    let expr = expr_ctx.plan_expression(context, expr)?;
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
                let mut expr = expr_ctx.plan_expression(context, expr)?;
                num_appended += Self::append_hidden(&mut select_exprs, &mut expr)?;
                Some(expr)
            }
            None => None,
        };

        let mut order_by_exprs = match order_by {
            Some(order_by) => order_by
                .order_by_nodes
                .into_iter()
                .map(|order_by| {
                    let expr = expr_ctx.plan_expression_with_select_list(
                        context,
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
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };

        num_appended += Self::append_order_by_exprs(&mut select_exprs, &mut order_by_exprs)?;

        // GROUP BY

        // Group by has access to everything we've planned so far.
        let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);
        plan.root = AggregatePlanner.plan(
            context,
            expr_ctx,
            &mut select_exprs,
            &alias_map,
            plan.root,
            select.group_by,
        )?;

        // Project the full select list.
        plan = LogicalQuery2 {
            root: LogicalOperator::Projection(Node::new(Projection {
                exprs: select_exprs.clone(),
                input: Box::new(plan.root),
            })),
            scope: plan.scope,
        };

        // Add filter for HAVING.
        if let Some(expr) = having_expr {
            plan = LogicalQuery2 {
                root: LogicalOperator::Filter2(Node::new(Filter {
                    predicate: expr,
                    input: Box::new(plan.root),
                })),
                scope: plan.scope,
            }
        }

        // Add order by node.
        if !order_by_exprs.is_empty() {
            plan = LogicalQuery2 {
                root: LogicalOperator::Order2(Node::new(Order {
                    exprs: order_by_exprs,
                    input: Box::new(plan.root),
                })),
                scope: plan.scope,
            }
        }

        // Turn select expressions back into only the expressions for the
        // output.
        if num_appended > 0 {
            let output_len = select_exprs.len() - num_appended;

            let projections = (0..output_len).map(LogicalExpression::new_column).collect();

            plan = LogicalQuery2 {
                root: LogicalOperator::Projection(Node::new(Projection {
                    exprs: projections,
                    input: Box::new(plan.root),
                })),
                scope: plan.scope,
            };
        }

        // Flatten subqueries;
        plan.root = SubqueryPlanner.flatten(context, plan.root)?;

        // Cleaned scope containing only output columns in the final output.
        plan.scope = Scope::with_columns(None, names);

        Ok(plan)
    }

    pub fn plan_from_node(
        &self,
        context: &mut QueryContext,
        from: ast::FromNode<ResolvedMeta>,
        current_schema: TypeSchema,
        current_scope: Scope,
    ) -> Result<LogicalQuery2> {
        // Plan the "body" of the FROM.
        let body = match from.body {
            ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference }) => {
                match self.bind_data.tables.try_get_bound(reference)? {
                    (
                        ResolvedTableOrCteReference::Table(ResolvedTableReference {
                            catalog,
                            schema,
                            entry,
                        }),
                        location,
                    ) => {
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
                            entry
                                .try_as_table_entry()?
                                .columns
                                .iter()
                                .map(|f| f.name.clone()),
                        );
                        LogicalQuery2 {
                            root: LogicalOperator::Scan2(Node::with_location(
                                Scan {
                                    catalog: catalog.clone(),
                                    schema: schema.clone(),
                                    source: entry.clone(),
                                },
                                location,
                            )),
                            scope,
                        }
                    }
                    (ResolvedTableOrCteReference::Cte(cte_idx), _) => {
                        self.plan_cte_body(context, *cte_idx, current_schema, current_scope)?
                    }
                }
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                let mut nested = self.nested(current_schema, current_scope);
                nested.plan_query(context, query)?
            }
            ast::FromNodeBody::TableFunction(ast::FromTableFunction { reference, .. }) => {
                let (reference, location) =
                    self.bind_data.table_functions.try_get_bound(reference)?;
                let scope_reference = TableReference {
                    database: None,
                    schema: None,
                    table: reference.name.clone(),
                };
                let func = reference.func.clone();
                let scope = Scope::with_columns(
                    Some(scope_reference),
                    func.schema().fields.into_iter().map(|f| f.name),
                );

                let operator = LogicalOperator::TableFunction(Node::with_location(
                    TableFunction { function: func },
                    location,
                ));

                LogicalQuery2 {
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
                let left_nested = self.nested(current_schema.clone(), current_scope.clone());
                let left_plan = left_nested.plan_from_node(
                    context,
                    *left,
                    TypeSchema::empty(),
                    Scope::empty(),
                )?; // TODO: Determine if should be empty.

                // Plan right side of join.
                //
                // Note this uses a plan context that has the "left" scope as
                // its outer scope.
                // TODO: Schema
                let right_nested = left_nested.nested(TypeSchema::empty(), left_plan.scope.clone());
                let right_plan = right_nested.plan_from_node(
                    context,
                    *right,
                    TypeSchema::empty(),
                    Scope::empty(),
                )?; // TODO: Determine if this should be empty.

                match join_condition {
                    ast::JoinCondition::On(on) => {
                        let merged = left_plan.scope.merge(right_plan.scope)?;
                        let left_schema = left_plan.root.output_schema(&[])?; // TODO: Outers
                        let right_schema = right_plan.root.output_schema(&[])?; // TODO: Outers
                        let merged_schema = left_schema.merge(right_schema);
                        let expr_ctx =
                            ExpressionContext::new(&left_nested, &merged, &merged_schema);

                        let on_expr = expr_ctx.plan_expression(context, on)?;

                        let join_type = match join_type {
                            ast::JoinType::Inner => JoinType::Inner,
                            ast::JoinType::Left => JoinType::Left,
                            ast::JoinType::Right => JoinType::Right,
                            ast::JoinType::Cross => {
                                unreachable!("Cross join should not have a join condition")
                            }
                            other => not_implemented!("plan join type: {other:?}"),
                        };

                        LogicalQuery2 {
                            root: LogicalOperator::AnyJoin(Node::new(AnyJoin {
                                left: Box::new(left_plan.root),
                                right: Box::new(right_plan.root),
                                join_type,
                                on: on_expr,
                            })),
                            scope: merged,
                        }
                    }
                    ast::JoinCondition::None => match join_type {
                        ast::JoinType::Cross => {
                            let merged = left_plan.scope.merge(right_plan.scope)?;

                            LogicalQuery2 {
                                root: LogicalOperator::CrossJoin(Node::new(CrossJoin {
                                    left: Box::new(left_plan.root),
                                    right: Box::new(right_plan.root),
                                })),
                                scope: merged,
                            }
                        }
                        _other => return Err(RayexecError::new("Missing join condition for join")),
                    },
                    using_or_natural => {
                        // let merged = left_plan.scope.merge(right_plan.scope)?;
                        let left_schema = left_plan.root.output_schema(&[])?; // TODO: Outers
                        let right_schema = right_plan.root.output_schema(&[])?; // TODO: Outers

                        let (left_on, right_on) = match using_or_natural {
                            ast::JoinCondition::Using(idents) => {
                                let left_ctx = ExpressionContext::new(
                                    &left_nested,
                                    &left_plan.scope,
                                    &left_schema,
                                );

                                let right_ctx = ExpressionContext::new(
                                    &right_nested,
                                    &right_plan.scope,
                                    &right_schema,
                                );

                                let mut left_on = Vec::new();
                                let mut right_on = Vec::new();

                                for ident in idents {
                                    let left_idx = left_ctx
                                        .plan_ident(ident.clone())?
                                        .try_into_column_ref()?
                                        .try_as_uncorrelated()?;
                                    let right_idx = right_ctx
                                        .plan_ident(ident)?
                                        .try_into_column_ref()?
                                        .try_as_uncorrelated()?;

                                    left_on.push(left_idx);
                                    right_on.push(right_idx);
                                }

                                (left_on, right_on)
                            }
                            ast::JoinCondition::Natural => {
                                not_implemented!("NATURAL join")
                            }
                            other => unreachable!(
                                "outer match statement checked join condition: {other:?}"
                            ),
                        };

                        // Compute projections where USING columns come first,
                        // then remaining left columns then remaining right.

                        let mut remaining_left = Vec::new();
                        for left_idx in 0..left_schema.types.len() {
                            if !left_on.contains(&left_idx) {
                                remaining_left.push(left_idx);
                            }
                        }

                        let mut remaining_right = Vec::new();
                        for right_idx in 0..right_schema.types.len() {
                            if !right_on.contains(&right_idx) {
                                remaining_right.push(right_idx);
                            }
                        }

                        // Compute merged scope.
                        //
                        // Use left on for the USING columns (these come first).
                        let using_scope_items = left_on
                            .iter()
                            .map(|&idx| left_plan.scope.items[idx].clone());

                        let left_remaining_scope_items = remaining_left
                            .iter()
                            .map(|&idx| left_plan.scope.items[idx].clone());

                        let right_remaining_scope_items = remaining_right
                            .iter()
                            .map(|&idx| right_plan.scope.items[idx].clone());

                        let merged: Vec<_> = using_scope_items
                            .chain(left_remaining_scope_items)
                            .chain(right_remaining_scope_items)
                            .collect();
                        let scope = Scope { items: merged };

                        // Projections
                        let right_offset = left_schema.types.len();
                        let projections: Vec<_> = left_on
                            .iter()
                            .map(|&idx| LogicalExpression::new_column(idx))
                            .chain(
                                remaining_left
                                    .into_iter()
                                    .map(|idx| LogicalExpression::new_column(idx)),
                            )
                            .chain(
                                remaining_right
                                    .into_iter()
                                    .map(|idx| LogicalExpression::new_column(idx + right_offset)),
                            )
                            .collect();

                        let join_type = match join_type {
                            ast::JoinType::Inner => JoinType::Inner,
                            ast::JoinType::Left => JoinType::Left,
                            ast::JoinType::Right => JoinType::Right,
                            ast::JoinType::Cross => {
                                unreachable!("Cross join should not have a join condition")
                            }
                            other => not_implemented!("plan join type: {other:?}"),
                        };

                        LogicalQuery2 {
                            root: LogicalOperator::Projection(Node::new(Projection {
                                exprs: projections,
                                input: Box::new(LogicalOperator::EqualityJoin(Node::new(
                                    EqualityJoin {
                                        left: Box::new(left_plan.root),
                                        right: Box::new(right_plan.root),
                                        join_type,
                                        left_on,
                                        right_on,
                                    },
                                ))),
                            })),
                            scope,
                        }
                    }
                }
            }
            ast::FromNodeBody::File(_) => {
                return Err(RayexecError::new(
                    "Resolver should have replace file path with a table function",
                ))
            }
        };

        // Apply aliases if provided.
        let aliased_scope = Self::apply_alias(body.scope, from.alias)?;

        Ok(LogicalQuery2 {
            root: body.root,
            scope: aliased_scope,
        })
    }

    /// Plans the body of a CTE, handling if the CTE is materialized or not.
    fn plan_cte_body(
        &self,
        context: &mut QueryContext,
        bound: CteIndex,
        current_schema: TypeSchema,
        current_scope: Scope,
    ) -> Result<LogicalQuery2> {
        let cte =
            self.bind_data.ctes.get(bound.0).ok_or_else(|| {
                RayexecError::new(format!("Missing bound CTE at index {}", bound.0))
            })?;

        if cte.materialized {
            // Check if we already have a materialized plan for
            // this CTE.
            if let Some(reference) = context.get_materialized_cte_reference(bound).cloned() {
                // We do, use it.
                // TODO: Zero clue what to use for outer.
                let scan = context.generate_scan_for_idx(reference.materialized_idx, &[])?;
                // TODO: I _think_ "any" location is fine for this, but
                // definitely needs to be double checked.
                return Ok(LogicalQuery2 {
                    root: LogicalOperator::MaterializedScan(Node::new(scan)),
                    scope: reference.scope,
                });
            }

            // Otherwise continue to plan it. We'll add the materialized plan at the end.
        }

        // Plan CTE body...
        let mut nested = self.nested(current_schema, current_scope);
        let mut query = nested.plan_query(context, cte.body.clone())?;

        let scope_reference = TableReference {
            database: None,
            schema: None,
            table: cte.name.clone(),
        };

        // TODO: Unsure how we want to set the scope for recursive yet.

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

        // If materialized, add it to the context and return a scan for it.
        if cte.materialized {
            let idx = context.push_materialized_cte(bound, query.root, query.scope.clone());
            let scan = context.generate_scan_for_idx(idx, &[])?; // TODO: Again not sure about outer.
            return Ok(LogicalQuery2 {
                root: LogicalOperator::MaterializedScan(Node::new(scan)),
                scope: query.scope,
            });
        }

        // Otherwise just return the plan as-is, it'll be inlined into the parent plan.
        Ok(query)
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

    fn plan_values(
        &self,
        context: &mut QueryContext,
        values: ast::Values<ResolvedMeta>,
    ) -> Result<LogicalQuery2> {
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
                    .map(|col_expr| expr_ctx.plan_expression(context, col_expr))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<Vec<LogicalExpression>>>>()?;

        let operator = LogicalOperator::ExpressionList(Node::new(ExpressionList { rows: exprs }));

        // Generate output scope with appropriate column names.
        let mut scope = Scope::empty();
        scope.add_columns(None, (0..num_cols).map(|i| format!("column{}", i + 1)));

        Ok(LogicalQuery2 {
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

    /// Applies casts to both components of a set operation.
    ///
    /// Errors if either the plans produce different number of outputs, or
    /// suitables casts cannot be found.
    fn apply_setop_casts(
        mut top: LogicalOperator,
        mut bottom: LogicalOperator,
        outer: &[TypeSchema],
    ) -> Result<[LogicalOperator; 2]> {
        let top_inputs = top.output_schema(outer)?;
        let bottom_inputs = bottom.output_schema(outer)?;

        if top_inputs.types.len() != bottom_inputs.types.len() {
            return Err(RayexecError::new(format!(
                "Inputs to set operations must produce the same number of columns, got {} and {}",
                top_inputs.types.len(),
                bottom_inputs.types.len()
            )));
        }

        #[derive(Debug, Clone, Copy)]
        enum CastPreference {
            /// Cast bottom to top.
            ToTop,
            /// Cast top to bottom.
            ToBottom,
            /// No cast needed.
            NotNeeded,
        }

        let mut casts = vec![CastPreference::NotNeeded; top_inputs.types.len()];

        for (idx, (top, bottom)) in top_inputs
            .types
            .iter()
            .zip(bottom_inputs.types.iter())
            .enumerate()
        {
            if top == bottom {
                // Nothing needed.
                continue;
            }

            let top_score = implicit_cast_score(bottom, top.datatype_id());
            let bottom_score = implicit_cast_score(top, bottom.datatype_id());

            if top_score == 0 && bottom_score == 0 {
                return Err(RayexecError::new(format!(
                    "Cannot find suitable type to cast to for column '{idx}'"
                )));
            }

            if top_score >= bottom_score {
                casts[idx] = CastPreference::ToTop;
            } else {
                casts[idx] = CastPreference::ToBottom;
            }
        }

        let top_cast_needed = casts
            .iter()
            .any(|pref| matches!(pref, CastPreference::ToBottom));
        let bottom_cast_needed = casts
            .iter()
            .any(|pref| matches!(pref, CastPreference::ToTop));

        if top_cast_needed {
            let mut projections = Vec::with_capacity(top_inputs.types.len());
            for (idx, pref) in casts.iter().enumerate() {
                if matches!(pref, CastPreference::ToBottom) {
                    projections.push(LogicalExpression::Cast {
                        to: bottom_inputs.types[idx].clone(),
                        expr: Box::new(LogicalExpression::new_column(idx)),
                    })
                } else {
                    projections.push(LogicalExpression::new_column(idx))
                }
            }

            top = LogicalOperator::Projection(Node::new(Projection {
                exprs: projections,
                input: Box::new(top),
            }))
        }

        if bottom_cast_needed {
            let mut projections = Vec::with_capacity(bottom_inputs.types.len());
            for (idx, pref) in casts.iter().enumerate() {
                if matches!(pref, CastPreference::ToTop) {
                    projections.push(LogicalExpression::Cast {
                        to: top_inputs.types[idx].clone(),
                        expr: Box::new(LogicalExpression::new_column(idx)),
                    })
                } else {
                    projections.push(LogicalExpression::new_column(idx))
                }
            }

            bottom = LogicalOperator::Projection(Node::new(Projection {
                exprs: projections,
                input: Box::new(bottom),
            }))
        }

        Ok([top, bottom])
    }
}
