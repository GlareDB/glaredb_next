use crate::{
    expr::scalar::{BinaryOperator, PlannedBinaryOperator},
    functions::aggregate::count::CountNonNullImpl,
    logical::operator::{
        Aggregate, CrossJoin, Limit, LogicalExpression, LogicalOperator, Projection,
    },
};
use rayexec_bullet::{datatype::DataType, scalar::OwnedScalarValue};
use rayexec_error::Result;

/// Logic for flattening and planning subqueries.
#[derive(Debug, Clone, Copy)]
pub struct SubqueryPlanner;

impl SubqueryPlanner {
    pub fn flatten(&self, mut plan: LogicalOperator) -> Result<LogicalOperator> {
        plan.walk_mut_post(&mut |plan| {
            match plan {
                LogicalOperator::Projection(p) => {
                    for expr in &mut p.exprs {
                        self.plan_subquery_expr(expr, &mut p.input)?;
                    }
                }
                LogicalOperator::Aggregate(p) => {
                    for expr in &mut p.aggregates {
                        self.plan_subquery_expr(expr, &mut p.input)?;
                    }
                }
                LogicalOperator::Filter(p) => {
                    self.plan_subquery_expr(&mut p.predicate, &mut p.input)?;
                }
                _other => (),
            };
            Ok(())
        })?;

        Ok(plan)
    }

    /// Plans a subquery expression with a logical operator input.
    ///
    /// Recursively transforms the expression the remove subqueries and place
    /// them in the plan.
    ///
    /// Does nothing if the expression isn't a subquery, or doesn't have a
    /// subquery as a child.
    pub fn plan_subquery_expr(
        &self,
        expr: &mut LogicalExpression,
        input: &mut LogicalOperator,
    ) -> Result<()> {
        let schema = input.output_schema(&[])?;
        let mut num_cols = schema.types.len();

        expr.walk_mut_post(&mut |expr| {
            if expr.is_subquery() {
                // TODO: Correlated check
                self.plan_uncorrelated(expr, input, num_cols)?;
                num_cols += 1;
            }
            Ok(())
        })?;

        Ok(())
    }

    fn plan_correlated(
        &self,
        expr: &mut LogicalExpression,
        input: &mut LogicalOperator,
        input_columns: usize,
    ) -> Result<()> {
        unimplemented!()
    }

    /// Plans a single uncorrelated subquery expression.
    ///
    /// The subquery will be flattened into the original input operator.
    ///
    /// `input_columns` is the number of columns that `input` will originally
    /// produce.
    fn plan_uncorrelated(
        &self,
        expr: &mut LogicalExpression,
        input: &mut LogicalOperator,
        input_columns: usize,
    ) -> Result<()> {
        match expr {
            expr @ LogicalExpression::Subquery(_) => {
                // Normal subquery.
                //
                // Cross join the subquery with the original input, replace
                // the subquery expression with a reference to the new
                // column.
                let column_ref = LogicalExpression::new_column(input_columns);
                let orig = std::mem::replace(expr, column_ref);
                let subquery = match orig {
                    LogicalExpression::Subquery(e) => e,
                    _ => unreachable!(),
                };

                // TODO: We should check that the subquery produces one
                // column around here.

                // LIMIT the original subquery to 1
                let subquery = LogicalOperator::Limit(Limit {
                    offset: None,
                    limit: 1,
                    input: subquery,
                });

                let orig_input = Box::new(std::mem::replace(input, LogicalOperator::Empty));
                *input = LogicalOperator::CrossJoin(CrossJoin {
                    left: orig_input,
                    right: Box::new(subquery),
                });
            }
            expr @ LogicalExpression::Exists { .. } => {
                // Exists subquery.
                //
                // EXISTS -> COUNT(*) == 1
                // NOT EXISTS -> COUNT(*) != 1
                //
                // Cross join with existing input. Replace original subquery expression
                // with reference to new column.

                let (subquery, not_exists) = match expr {
                    LogicalExpression::Exists {
                        not_exists,
                        subquery,
                    } => {
                        let subquery =
                            std::mem::replace(subquery, Box::new(LogicalOperator::Empty));
                        (subquery, not_exists)
                    }
                    _ => unreachable!("variant checked in outer match"),
                };

                *expr = LogicalExpression::Binary {
                    op: if *not_exists {
                        PlannedBinaryOperator {
                            op: BinaryOperator::NotEq,
                            scalar: BinaryOperator::NotEq
                                .scalar_function()
                                .plan_from_datatypes(&[DataType::Int64, DataType::Int64])?,
                        }
                    } else {
                        PlannedBinaryOperator {
                            op: BinaryOperator::Eq,
                            scalar: BinaryOperator::Eq
                                .scalar_function()
                                .plan_from_datatypes(&[DataType::Int64, DataType::Int64])?,
                        }
                    },
                    left: Box::new(LogicalExpression::new_column(input_columns)),
                    right: Box::new(LogicalExpression::Literal(OwnedScalarValue::Int64(1))),
                };

                // COUNT(*) and LIMIT the original query.
                let subquery = LogicalOperator::Aggregate(Aggregate {
                    // TODO: Replace with CountStar once that's in.
                    //
                    // This currently just includes a 'true'
                    // projection that makes the final aggregate
                    // represent COUNT(true).
                    aggregates: vec![LogicalExpression::Aggregate {
                        agg: Box::new(CountNonNullImpl),
                        inputs: vec![LogicalExpression::new_column(0)],
                        filter: None,
                    }],
                    grouping_sets: None,
                    group_exprs: Vec::new(),
                    input: Box::new(LogicalOperator::Limit(Limit {
                        offset: None,
                        limit: 1,
                        input: Box::new(LogicalOperator::Projection(Projection {
                            exprs: vec![LogicalExpression::Literal(OwnedScalarValue::Boolean(
                                true,
                            ))],
                            input: subquery,
                        })),
                    })),
                });

                let orig_input = Box::new(std::mem::replace(input, LogicalOperator::Empty));
                *input = LogicalOperator::CrossJoin(CrossJoin {
                    left: orig_input,
                    right: Box::new(subquery),
                });
            }
            _ => (),
        }
        Ok(())
    }
}
