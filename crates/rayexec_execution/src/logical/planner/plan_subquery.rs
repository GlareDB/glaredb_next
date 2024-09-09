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
        logical_join::LogicalCrossJoin,
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
use std::collections::HashMap;

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
                    // not_implemented!("correlated subqueries");
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

    fn plan_correlated(
        &self,
        bind_context: &mut BindContext,
        subquery: &mut SubqueryExpr,
        plan: &mut LogicalOperator,
    ) -> Result<Expression> {
        let subquery_plan = QueryPlanner.plan(bind_context, subquery.subquery.as_ref().clone())?;
        let correlated_columns = bind_context.correlated_columns(subquery.bind_idx)?.clone();

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

                // Flatten the right side. This assumes we're doing a dependent
                // join with left. The goal is after flattening here, the join
                // we make at the end _shouldn't_ be a dependent join, but just
                // a normal comparison join.
                let right = Self::dependent_join_pushdown(subquery_plan, &correlated_columns)?;
            }
            _ => unimplemented!(),
        }

        unimplemented!()
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

    fn eliminate_duplicates(
        plan: LogicalOperator,
        correlated_cols: &[CorrelatedColumn],
    ) -> LogicalOperator {
        let exprs = correlated_cols
            .iter()
            .map(|col| {
                Expression::Column(ColumnExpr {
                    table_scope: col.table,
                    column: col.col_idx,
                })
            })
            .collect();

        LogicalOperator::Distinct(Node {
            node: LogicalDistinct { on: exprs },
            location: LocationRequirement::Any,
            children: vec![plan],
        })
    }

    fn dependent_join_pushdown(
        plan: LogicalOperator,
        correlated_cols: &[CorrelatedColumn],
    ) -> Result<LogicalOperator> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct LogicalOperatorRef<'a>(&'a LogicalOperator);

impl<'a> std::hash::Hash for LogicalOperatorRef<'a> {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        (self.0 as *const LogicalOperator).hash(state)
    }
}

impl<'a, 'b> PartialEq<LogicalOperatorRef<'b>> for LogicalOperatorRef<'a> {
    fn eq(&self, other: &LogicalOperatorRef<'b>) -> bool {
        self.0 as *const LogicalOperator == other.0 as *const LogicalOperator
    }
}

impl<'a> Eq for LogicalOperatorRef<'a> {}

#[derive(Debug)]
struct DependentJoinPushdown<'a> {
    /// Reference to the materialized plan on the left side.
    mat_ref: MaterializationRef,
    correlated_operators: HashMap<LogicalOperatorRef<'a>, bool>,
    /// Map correlated columns to updated column expressions.
    column_map: HashMap<CorrelatedColumn, ColumnExpr>,
    columns: Vec<CorrelatedColumn>,
}

impl<'a> DependentJoinPushdown<'a> {
    fn find_correlations(&mut self, plan: &'a LogicalOperator) -> Result<bool> {
        let mut has_correlation = false;
        match plan {
            LogicalOperator::Project(project) => {
                has_correlation = self.any_expression_has_correlation(&project.node.projections);
                has_correlation |= self.find_correlations_in_children(&project.children)?;
            }
            _ => (),
        }

        self.correlated_operators
            .insert(LogicalOperatorRef(plan), has_correlation);

        Ok(has_correlation)
    }

    fn find_correlations_in_children(&mut self, children: &'a [LogicalOperator]) -> Result<bool> {
        let mut child_has_correlation = false;
        for child in children {
            child_has_correlation |= self.find_correlations(child)?;
        }
        Ok(child_has_correlation)
    }

    fn pushdown(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let has_correlation = *self
            .correlated_operators
            .get(&LogicalOperatorRef(&plan))
            .unwrap_or(&false);

        if !has_correlation {
            // Operator (and children) do not have correlated columns. Cross
            // join with materialized scan with duplicates eliminated.

            let plan_tables = plan.get_output_table_refs();
            let mut mappings = Vec::new();

            let materialization = bind_context.get_materialization(self.mat_ref)?;

            for correlated in self.columns.iter() {
                // Only take into account columns that reference the underlying
                // materialization.
                if !plan_tables.contains(&correlated.table) {
                    continue;
                }

                // Push a mapping of correlated -> materialized column.
                //
                // This uses the original correlated column index since the
                // materialized scan outputs the entire original projection, and
                // the correlation might just be on one column.
                //
                // TODO: Assumes input to materialization is a single table ref.
                // Probably need to update materialization to use the original
                // table refs.
                mappings.push((
                    correlated,
                    ColumnExpr {
                        table_scope: materialization.table_ref,
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

            let left = LogicalOperator::Distinct(Node {
                node: LogicalDistinct { on: distinct_on },
                location: LocationRequirement::Any,
                children: vec![LogicalOperator::MaterializationScan(Node {
                    node: LogicalMaterializationScan { mat: self.mat_ref },
                    location: LocationRequirement::Any,
                    children: Vec::new(),
                })],
            });

            return Ok(LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![left, plan],
            }));
        }

        match plan {
            LogicalOperator::Project(mut project) => {
                project.children = self.pushdown_children(bind_context, project.children)?;
                self.rewrite_expressions(&mut project.node.projections)?;

                // TODO: Need to update column map to point to this projection
                // for successfully updated columns.

                Ok(LogicalOperator::Project(project))
            }
            LogicalOperator::Scan(scan) => {
                if matches!(
                    scan.node.source,
                    ScanSource::Table { .. }
                        | ScanSource::View { .. }
                        | ScanSource::TableFunction { .. }
                ) {
                    // Nothing to do.
                    return Ok(LogicalOperator::Scan(scan));
                }

                not_implemented!("dependent join pushdown for VALUES")
            }
            other => not_implemented!("dependent join pushdown for node: {other:?}"),
        }
    }

    fn pushdown_children(
        &mut self,
        bind_context: &mut BindContext,
        children: Vec<LogicalOperator>,
    ) -> Result<Vec<LogicalOperator>> {
        children
            .into_iter()
            .map(|c| self.pushdown(bind_context, c))
            .collect::<Result<Vec<_>>>()
    }

    fn any_expression_has_correlation(&self, exprs: &[Expression]) -> bool {
        exprs.iter().any(|e| self.expression_has_correlation(e))
    }

    fn expression_has_correlation(&self, expr: &Expression) -> bool {
        match expr {
            Expression::Column(col) => self
                .columns
                .iter()
                .any(|c| c.table == col.table_scope && c.col_idx == col.column),
            other => self.expression_has_correlation(other),
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

                    *expr = Expression::Column(new_col.clone());
                }

                // Column we're not concerned about. Remains unchanged.
                Ok(())
            }
            other => other.for_each_child_mut(&mut |child| self.rewrite_expression(child)),
        }
    }
}
