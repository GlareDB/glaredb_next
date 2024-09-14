use crate::{
    expr::{
        aggregate_expr::AggregateExpr,
        column_expr::ColumnExpr,
        comparison_expr::{ComparisonExpr, ComparisonOperator},
        literal_expr::LiteralExpr,
        subquery_expr::{SubqueryExpr, SubqueryType},
        Expression,
    },
    functions::aggregate::count::CountNonNullImpl,
    logical::{
        binder::bind_context::{BindContext, CorrelatedColumn, MaterializationRef},
        logical_aggregate::LogicalAggregate,
        logical_distinct::LogicalDistinct,
        logical_join::{ComparisonCondition, JoinType, LogicalComparisonJoin, LogicalCrossJoin},
        logical_limit::LogicalLimit,
        logical_materialization::LogicalMaterializationScan,
        logical_project::LogicalProject,
        logical_scan::ScanSource,
        operator::{LocationRequirement, LogicalNode, LogicalOperator, Node},
        planner::plan_query::QueryPlanner,
    },
};
use rayexec_bullet::{datatype::DataType, scalar::ScalarValue};
use rayexec_error::{not_implemented, RayexecError, Result};
use std::collections::{BTreeSet, HashMap};

#[derive(Debug)]
pub struct SubqueryPlanner;

impl SubqueryPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        expr: &mut Expression,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        self.plan_inner(bind_context, expr, &mut plan)?;
        Ok(plan)
    }

    fn plan_inner(
        &self,
        bind_context: &mut BindContext,
        expr: &mut Expression,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        match expr {
            Expression::Subquery(subquery) => {
                if subquery.has_correlations(bind_context)? {
                    *expr = self.plan_correlated(bind_context, subquery, plan)?
                } else {
                    *expr = self.plan_uncorrelated(bind_context, subquery, plan)?
                }
            }
            other => other.for_each_child_mut(&mut |expr| {
                self.plan_inner(bind_context, expr, plan)?;
                Ok(())
            })?,
        }

        Ok(())
    }

    /// Plans a correlated subquery.
    ///
    /// This will attempt to decorrelate the subquery, modifying `plan` to do
    /// so. The returned expression should then be used in place of the original
    /// subquery expression.
    ///
    /// Decorrelation follows the logic described in "Unnesting Arbitrary
    /// Queries" (Neumann, Kemper):
    ///
    /// <https://btw-2015.informatik.uni-hamburg.de/res/proceedings/Hauptband/Wiss/Neumann-Unnesting_Arbitrary_Querie.pdf>
    fn plan_correlated(
        &self,
        bind_context: &mut BindContext,
        subquery: &mut SubqueryExpr,
        plan: &mut LogicalOperator,
    ) -> Result<Expression> {
        let mut subquery_plan =
            QueryPlanner.plan(bind_context, subquery.subquery.as_ref().clone())?;

        // Get only correlated columns that are pointing to this plan.
        let plan_tables = plan.get_output_table_refs();

        let correlated_columns: Vec<_> = bind_context
            .correlated_columns(subquery.bind_idx)?
            .iter()
            .filter(|c| plan_tables.contains(&c.table))
            .cloned()
            .collect();

        match subquery.subquery_type {
            SubqueryType::Scalar => {
                // Create dependent join between left (original query) and right
                // (subquery). Left requires duplication elimination on the
                // correlated columns.
                //
                // The resulting plan may have nodes scanning from the left
                // multiple times.

                let orig = std::mem::replace(plan, LogicalOperator::Invalid);
                let mat_ref = bind_context.new_materialization(orig)?;

                let left = LogicalOperator::MaterializationScan(Node {
                    node: LogicalMaterializationScan {
                        mat: mat_ref,
                        table_refs: plan_tables,
                    },
                    location: LocationRequirement::Any,
                    children: Vec::new(),
                });
                bind_context.inc_materialization_scan_count(mat_ref, 1)?;

                // Flatten the right side. This assumes we're doing a dependent
                // join with left. The goal is after flattening here, the join
                // we make at the end _shouldn't_ be a dependent join, but just
                // a normal comparison join.
                let mut planner = DependentJoinPushdown::new(mat_ref, correlated_columns);

                planner.find_correlations(&subquery_plan)?;
                planner.pushdown(bind_context, &mut subquery_plan)?;

                // Make comparison join between left & right using the updated
                // column map from the push down.

                let mut conditions = Vec::with_capacity(planner.columns.len());
                for correlated in planner.columns {
                    // Correlated points to left, the materialized side.
                    let left = Expression::Column(ColumnExpr {
                        table_scope: correlated.table,
                        column: correlated.col_idx,
                    });

                    let right = planner.column_map.get(&correlated).ok_or_else(|| {
                        RayexecError::new(format!(
                            "Missing updated right side for correlate column: {correlated:?}"
                        ))
                    })?;

                    conditions.push(ComparisonCondition {
                        left,
                        right: Expression::Column(*right),
                        op: ComparisonOperator::Eq,
                    });
                }

                // Result expression for the subquery, output of the right side
                // of the join.
                let right_out = Expression::Column(ColumnExpr {
                    table_scope: subquery_plan.get_output_table_refs()[0],
                    column: 0,
                });

                // Update plan to now be a comparison join.
                *plan = LogicalOperator::ComparisonJoin(Node {
                    node: LogicalComparisonJoin {
                        join_type: JoinType::Left,
                        conditions,
                    },
                    location: LocationRequirement::Any,
                    children: vec![left, subquery_plan],
                });

                Ok(right_out)
            }
            other => not_implemented!("correlated subquery type: {other:?}"),
        }
    }

    fn plan_uncorrelated(
        &self,
        bind_context: &mut BindContext,
        subquery: &mut SubqueryExpr,
        plan: &mut LogicalOperator,
    ) -> Result<Expression> {
        // Generate subquery logical plan.
        let subquery_plan = QueryPlanner.plan(bind_context, subquery.subquery.as_ref().clone())?;

        match subquery.subquery_type {
            SubqueryType::Scalar => {
                // Normal subquery.
                //
                // Cross join the subquery with the original input, replace
                // the subquery expression with a reference to the new
                // column.

                // Generate column expr that references the scalar being joined
                // to the plan.
                let subquery_table = subquery_plan.get_output_table_refs()[0];
                let column = ColumnExpr {
                    table_scope: subquery_table,
                    column: 0,
                };

                // Limit original subquery to only one row.
                let subquery_plan = LogicalOperator::Limit(Node {
                    node: LogicalLimit {
                        offset: None,
                        limit: 1,
                    },
                    location: LocationRequirement::Any,
                    children: vec![subquery_plan],
                });

                // Cross join!
                let orig = std::mem::replace(plan, LogicalOperator::Invalid);
                *plan = LogicalOperator::CrossJoin(Node {
                    node: LogicalCrossJoin,
                    location: LocationRequirement::Any,
                    children: vec![orig, subquery_plan],
                });

                Ok(Expression::Column(column))
            }
            SubqueryType::Exists { negated } => {
                // Exists subquery.
                //
                // EXISTS -> COUNT(*) == 1
                // NOT EXISTS -> COUNT(*) != 1
                //
                // Cross join with existing input. Replace original subquery expression
                // with reference to new column.

                let subquery_table = subquery_plan.get_output_table_refs()[0];
                let subquery_column = ColumnExpr {
                    table_scope: subquery_table,
                    column: 0,
                };

                let agg_table = bind_context.new_ephemeral_table()?;
                bind_context.push_column_for_table(
                    agg_table,
                    "__generated_count",
                    DataType::Int64,
                )?;

                let projection_table = bind_context.new_ephemeral_table()?;
                bind_context.push_column_for_table(
                    projection_table,
                    "__generated_exists",
                    DataType::Boolean,
                )?;

                let subquery_exists_plan = LogicalOperator::Project(Node {
                    node: LogicalProject {
                        projections: vec![Expression::Comparison(ComparisonExpr {
                            left: Box::new(Expression::Column(ColumnExpr {
                                table_scope: agg_table,
                                column: 0,
                            })),
                            right: Box::new(Expression::Literal(LiteralExpr {
                                literal: ScalarValue::Int64(1),
                            })),
                            op: if negated {
                                ComparisonOperator::NotEq
                            } else {
                                ComparisonOperator::Eq
                            },
                        })],
                        projection_table,
                    },
                    location: LocationRequirement::Any,
                    children: vec![LogicalOperator::Aggregate(Node {
                        node: LogicalAggregate {
                            aggregates_table: agg_table,
                            aggregates: vec![Expression::Aggregate(AggregateExpr {
                                agg: Box::new(CountNonNullImpl),
                                inputs: vec![Expression::Column(subquery_column)],
                                filter: None,
                            })],
                            group_table: None,
                            group_exprs: Vec::new(),
                            grouping_sets: None,
                        },
                        location: LocationRequirement::Any,
                        children: vec![LogicalOperator::Limit(Node {
                            node: LogicalLimit {
                                offset: None,
                                limit: 1,
                            },
                            location: LocationRequirement::Any,
                            children: vec![subquery_plan],
                        })],
                    })],
                });

                let orig = std::mem::replace(plan, LogicalOperator::Invalid);
                *plan = LogicalOperator::CrossJoin(Node {
                    node: LogicalCrossJoin,
                    location: LocationRequirement::Any,
                    children: vec![orig, subquery_exists_plan],
                });

                // Return column referencing the project.
                Ok(Expression::Column(ColumnExpr {
                    table_scope: projection_table,
                    column: 0,
                }))
            }
            other => not_implemented!("subquery type {other:?}"),
        }
    }
}

/// Wrapper around a logical operator pointer for hashing the pointer.
///
/// This is used to allow us to walk the plan determining if subtrees contain
/// correlated queries without needing to store the operator.
///
/// This may or may not be smart. I don't know yet.
#[derive(Debug)]
struct LogicalOperatorPtr(*const LogicalOperator);

impl LogicalOperatorPtr {
    fn new(plan: &LogicalOperator) -> Self {
        LogicalOperatorPtr(plan as _)
    }
}

impl std::hash::Hash for LogicalOperatorPtr {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        self.0.hash(state)
    }
}

impl PartialEq<LogicalOperatorPtr> for LogicalOperatorPtr {
    fn eq(&self, other: &LogicalOperatorPtr) -> bool {
        self.0 == other.0
    }
}

impl Eq for LogicalOperatorPtr {}

/// Contains logic for pushing down a dependent join in a logical such that the
/// resulting plan does not have a dependent join.
#[derive(Debug)]
struct DependentJoinPushdown {
    /// Reference to the materialized plan on the left side.
    mat_ref: MaterializationRef,
    /// Holds pointers to nodes in the plan indicating if it or any of its
    /// children contains a correlated column.
    correlated_operators: HashMap<LogicalOperatorPtr, bool>,
    /// Map correlated columns to updated column expressions.
    ///
    /// This is updated as we walk back up the plan to allow expressions further
    /// up the tree to be rewritten to point to now decorrelated columns.
    column_map: HashMap<CorrelatedColumn, ColumnExpr>,
    /// List of correlated columns we're looking for in the plan.
    columns: Vec<CorrelatedColumn>,
}

impl DependentJoinPushdown {
    fn new(mat_ref: MaterializationRef, columns: Vec<CorrelatedColumn>) -> Self {
        // Initial column map points to itself.
        let column_map: HashMap<_, _> = columns
            .iter()
            .map(|c| {
                (
                    c.clone(),
                    ColumnExpr {
                        table_scope: c.table,
                        column: c.col_idx,
                    },
                )
            })
            .collect();

        DependentJoinPushdown {
            mat_ref,
            correlated_operators: HashMap::new(),
            column_map,
            columns,
        }
    }

    /// Walk the logical plan and find correlations that we need to handle
    /// during pushdown.
    fn find_correlations(&mut self, plan: &LogicalOperator) -> Result<bool> {
        let mut has_correlation = false;
        match plan {
            LogicalOperator::Project(project) => {
                has_correlation = self.any_expression_has_correlation(&project.node.projections);
                has_correlation |= self.find_correlations_in_children(&project.children)?;
            }
            LogicalOperator::Filter(filter) => {
                has_correlation = self.expression_has_correlation(&filter.node.filter);
                has_correlation |= self.find_correlations_in_children(&filter.children)?;
            }
            LogicalOperator::Aggregate(agg) => {
                has_correlation = self.any_expression_has_correlation(&agg.node.aggregates);
                has_correlation |= self.any_expression_has_correlation(&agg.node.group_exprs);
                has_correlation |= self.find_correlations_in_children(&agg.children)?;
            }
            LogicalOperator::CrossJoin(join) => {
                // TODO: Implement the push down
                has_correlation = self.find_correlations_in_children(&join.children)?;
            }
            LogicalOperator::ArbitraryJoin(join) => {
                // TODO: Implement the push down
                has_correlation = self.expression_has_correlation(&join.node.condition);
                has_correlation |= self.find_correlations_in_children(&join.children)?
            }
            LogicalOperator::ComparisonJoin(join) => {
                // TODO: Implement the push down
                has_correlation = self.any_expression_has_correlation(
                    join.node
                        .conditions
                        .iter()
                        .flat_map(|c| [&c.left, &c.right].into_iter()),
                );
                has_correlation |= self.find_correlations_in_children(&join.children)?;
            }
            LogicalOperator::Limit(_) => {
                // Limit should not have correlations.
            }
            LogicalOperator::Order(order) => {
                // TODO: Implement the push down
                has_correlation =
                    self.any_expression_has_correlation(order.node.exprs.iter().map(|e| &e.expr));
                has_correlation |= self.find_correlations_in_children(&order.children)?;
            }
            _ => (),
        }

        self.correlated_operators
            .insert(LogicalOperatorPtr::new(plan), has_correlation);

        Ok(has_correlation)
    }

    fn find_correlations_in_children(&mut self, children: &[LogicalOperator]) -> Result<bool> {
        let mut child_has_correlation = false;
        for child in children {
            child_has_correlation |= self.find_correlations(child)?;
        }
        Ok(child_has_correlation)
    }

    /// Pushes down a conceptual dependent join.
    ///
    /// Note that there's no explicit "dependent join" operator, and this is
    /// just acting as if there was one. The resulting plan should contain a
    /// cross join against the materialized original plan, with all correlated
    /// columns resolved against that cross join.
    ///
    /// Also Sean decide that we hash pointers, so this takes a mut reference to
    /// the plan and modify in place instead of returning a new plan. This
    /// reference should be the same one used for `find_correlations`, otherwise
    /// an error occurs.
    fn pushdown(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        let has_correlation = *self
            .correlated_operators
            .get(&LogicalOperatorPtr::new(plan))
            .ok_or_else(|| {
                RayexecError::new(format!("Missing correlation check for plan: {plan:?}"))
            })?;

        if !has_correlation {
            // Operator (and children) do not have correlated columns. Cross
            // join with materialized scan with duplicates eliminated.

            let mut mappings = Vec::new();
            for correlated in self.columns.iter() {
                // Push a mapping of correlated -> materialized column.
                //
                // This uses the original correlated column info since the
                // column should already be pointing to the output of the
                // materialization.
                //
                // As we walk back up the tree, the mappings will be updated to
                // point to the appropriate column.
                mappings.push((
                    correlated,
                    ColumnExpr {
                        table_scope: correlated.table,
                        column: correlated.col_idx,
                    },
                ))
            }

            // Update mapping.
            for (corr, expr) in mappings.clone() {
                self.column_map.insert(corr.clone(), expr);
            }

            // Distinct on only the correlated columns, since that's what the
            // subquery actually cares about.
            //
            // TODO: Maybe the distinct should be in the materialization
            // instead? I think a more specializated materialization scheme
            // needs to be added since the original plan may included
            // duplicates, but the plan being fed into the subquery needs all
            // duplicated removed (on the correlated columns).
            let distinct_on = mappings
                .into_iter()
                .map(|(_, expr)| Expression::Column(expr))
                .collect();

            // Note this distinct is reading from the left side of the query,
            // but being placed on the right side of the join. This is to make
            // rewriting operators (projections) further up this subtree easier.
            //
            // For projections, we have to to ensure that there's column exprs
            // that point to the materialized node, and be having the
            // materialization on the right, we can just append the expressions.
            let materialization = bind_context.get_materialization(self.mat_ref)?;
            let right = LogicalOperator::Distinct(Node {
                node: LogicalDistinct { on: distinct_on },
                location: LocationRequirement::Any,
                children: vec![LogicalOperator::MaterializationScan(Node {
                    node: LogicalMaterializationScan {
                        mat: self.mat_ref,
                        table_refs: materialization.table_refs.clone(),
                    },
                    location: LocationRequirement::Any,
                    children: Vec::new(),
                })],
            });
            bind_context.inc_materialization_scan_count(self.mat_ref, 1)?;

            let orig = std::mem::replace(plan, LogicalOperator::Invalid);

            *plan = LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![orig, right],
            });

            return Ok(());
        }

        match plan {
            LogicalOperator::Project(project) => {
                self.pushdown_children(bind_context, &mut project.children)?;
                self.rewrite_expressions(&mut project.node.projections)?;

                // Append column exprs referencing the materialization.
                let offset = project.node.projections.len();
                for (idx, correlated) in self.columns.iter().enumerate() {
                    let expr =
                        Expression::Column(*self.column_map.get(correlated).ok_or_else(|| {
                            RayexecError::new(
                                format!("Missing correlated column in column map for appending projection: {correlated:?}"))
                        })?);

                    // Append column to table in bind context.
                    bind_context.push_column_for_table(
                        project.node.projection_table,
                        format!("__generated_projection_decorrelation_{idx}"),
                        expr.datatype(bind_context)?,
                    )?;

                    project.node.projections.push(expr);

                    self.column_map.insert(
                        correlated.clone(),
                        ColumnExpr {
                            table_scope: project.node.projection_table,
                            column: offset + idx,
                        },
                    );
                }

                Ok(())
            }
            LogicalOperator::Filter(filter) => {
                self.pushdown_children(bind_context, &mut filter.children)?;
                self.rewrite_expression(&mut filter.node.filter)?;

                // Filter does not change columns that can be referenced by
                // parent nodes, don't update column map.

                Ok(())
            }
            LogicalOperator::Aggregate(agg) => {
                self.pushdown_children(bind_context, &mut agg.children)?;
                self.rewrite_expressions(&mut agg.node.aggregates)?;
                self.rewrite_expressions(&mut agg.node.group_exprs)?;

                // Append correlated columns to group by expressions.
                let offset = agg.node.group_exprs.len();

                // If we don't have a table ref for the group by (indicating we
                // have no groups), go ahead and create it.
                let group_by_table = match agg.node.group_table {
                    Some(table) => table,
                    None => {
                        let table = bind_context.new_ephemeral_table()?;
                        agg.node.group_table = Some(table);
                        table
                    }
                };

                // Same as above, we're always going to have groups.
                let grouping_sets = match &mut agg.node.grouping_sets {
                    Some(sets) => sets,
                    None => {
                        // Create single group.
                        agg.node.grouping_sets = Some(vec![BTreeSet::new()]);
                        agg.node.grouping_sets.as_mut().unwrap()
                    }
                };

                for (idx, correlated) in self.columns.iter().enumerate() {
                    let expr =
                        Expression::Column(*self.column_map.get(correlated).ok_or_else(|| {
                            RayexecError::new(
                                format!("Missing correlated column in column map for appending group expression: {correlated:?}"))
                        })?);

                    // Append column to group by table in bind context.
                    bind_context.push_column_for_table(
                        group_by_table,
                        format!("__generated_aggregate_decorrelation_{idx}"),
                        expr.datatype(bind_context)?,
                    )?;

                    // Add to group by.
                    agg.node.group_exprs.push(expr);
                    // Add to all grouping sets too.
                    for set in grouping_sets.iter_mut() {
                        set.insert(offset + idx);
                    }

                    // Update column map to point to expression in GROUP BY.
                    self.column_map.insert(
                        correlated.clone(),
                        ColumnExpr {
                            table_scope: group_by_table,
                            column: offset + idx,
                        },
                    );
                }

                Ok(())
            }
            LogicalOperator::Scan(scan) => {
                if matches!(
                    scan.node.source,
                    ScanSource::Table { .. }
                        | ScanSource::View { .. }
                        | ScanSource::TableFunction { .. }
                ) {
                    return Err(RayexecError::new(
                        "Unexpectedly reached scan node when pushing down dependent join",
                    ));
                }

                not_implemented!("dependent join pushdown for VALUES")
            }
            other => not_implemented!("dependent join pushdown for node: {other:?}"),
        }
    }

    fn pushdown_children(
        &mut self,
        bind_context: &mut BindContext,
        children: &mut [LogicalOperator],
    ) -> Result<()> {
        for child in children {
            self.pushdown(bind_context, child)?;
        }
        Ok(())
    }

    fn any_expression_has_correlation<'a>(
        &self,
        exprs: impl IntoIterator<Item = &'a Expression>,
    ) -> bool {
        exprs
            .into_iter()
            .any(|e| self.expression_has_correlation(e))
    }

    fn expression_has_correlation(&self, expr: &Expression) -> bool {
        match expr {
            Expression::Column(col) => self
                .columns
                .iter()
                .any(|c| c.table == col.table_scope && c.col_idx == col.column),
            other => {
                let mut has_correlation = false;
                other
                    .for_each_child(&mut |child| {
                        if has_correlation {
                            return Ok(());
                        }
                        has_correlation = self.expression_has_correlation(child);
                        Ok(())
                    })
                    .expect("expr correlation walk to not fail");
                has_correlation
            }
        }
    }

    fn rewrite_expressions(&self, exprs: &mut [Expression]) -> Result<()> {
        for expr in exprs {
            self.rewrite_expression(expr)?;
        }
        Ok(())
    }

    fn rewrite_expression(&self, expr: &mut Expression) -> Result<()> {
        match expr {
            Expression::Column(col) => {
                if let Some(correlated) = self
                    .columns
                    .iter()
                    .find(|corr| corr.table == col.table_scope && corr.col_idx == col.column)
                {
                    // Correlated column found, update to mapped column.
                    let new_col = self.column_map.get(correlated).ok_or_else(|| {
                        RayexecError::new(format!(
                            "Missing correlated column in column map: {correlated:?}"
                        ))
                    })?;

                    *expr = Expression::Column(*new_col);
                }

                // Column we're not concerned about. Remains unchanged.
                Ok(())
            }
            other => other.for_each_child_mut(&mut |child| self.rewrite_expression(child)),
        }
    }
}
