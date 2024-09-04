use crate::{
    expr::{physical::PhysicalScalarExpression, AsScalarFunction, Expression},
    logical::{
        binder::{
            bind_context::{BindContext, TableRef},
            bind_query::bind_modifier::BoundOrderByExpr,
        },
        logical_join::ComparisonCondition,
    },
};
use fmtutil::IntoDisplayableSlice;
use rayexec_error::{not_implemented, RayexecError, Result};

use super::{
    cast_expr::PhysicalCastExpr, column_expr::PhysicalColumnExpr,
    literal_expr::PhysicalLiteralExpr, scalar_function_expr::PhysicalScalarFunctionExpr,
    PhysicalAggregateExpression, PhysicalSortExpression,
};

/// Plans logical expressions into their physical equivalents.
#[derive(Debug)]
pub struct PhysicalExpressionPlanner<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> PhysicalExpressionPlanner<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        PhysicalExpressionPlanner { bind_context }
    }

    /// Plan more than one scalar expression.
    pub fn plan_scalars(
        &self,
        table_refs: &[TableRef],
        exprs: &[Expression],
    ) -> Result<Vec<PhysicalScalarExpression>> {
        exprs
            .iter()
            .map(|expr| self.plan_scalar(table_refs, expr))
            .collect::<Result<Vec<_>>>()
    }

    /// Plans a physical scalar expressions.
    ///
    /// Tables refs is a list of table references that represent valid
    /// expression inputs into some plan. For example, a join will have two
    /// table refs, left and right. Column expression may reference either the
    /// left or right table. If the expression does not reference a table, it
    /// indicates we didn't properly decorrelate the expression, and we error.
    ///
    /// The output expression list assumes that the input into an operator is a
    /// flat batch of columns. This means for a join, the batch will represent
    /// [left, right] table refs, and so column references on the right will
    /// take into account the number of columns on left.
    pub fn plan_scalar(
        &self,
        table_refs: &[TableRef],
        expr: &Expression,
    ) -> Result<PhysicalScalarExpression> {
        match expr {
            Expression::Column(col) => {
                // TODO: How is projection pushdown going to work? Will tables
                // be updated by the optimizer?

                let mut offset = 0;
                for &table_ref in table_refs {
                    let table = self.bind_context.get_table(table_ref)?;

                    if col.table_scope == table_ref {
                        return Ok(PhysicalScalarExpression::Column(PhysicalColumnExpr {
                            idx: offset + col.column,
                        }));
                    }

                    offset += table.num_columns();
                }

                // Column not in any of our required tables, indicates
                // correlated column.
                Err(RayexecError::new(
                    format!(
                        "Column expr not referencing a valid table ref, column: {col}, valid tables: {}",
                        table_refs.display_with_brackets(),
                    )
                ))
            }
            Expression::Literal(expr) => {
                Ok(PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                    literal: expr.literal.clone(),
                }))
            }
            Expression::ScalarFunction(expr) => Ok(PhysicalScalarExpression::ScalarFunction(
                PhysicalScalarFunctionExpr {
                    function: expr.function.clone(),
                    inputs: self.plan_scalars(table_refs, &expr.inputs)?,
                },
            )),
            Expression::Cast(expr) => Ok(PhysicalScalarExpression::Cast(PhysicalCastExpr {
                to: expr.to.clone(),
                expr: Box::new(self.plan_scalar(table_refs, &expr.expr)?),
            })),
            Expression::Comparison(expr) => {
                let scalar = expr.op.as_scalar_function();
                let function =
                    scalar.plan_from_expressions(self.bind_context, &[&expr.left, &expr.right])?;

                Ok(PhysicalScalarExpression::ScalarFunction(
                    PhysicalScalarFunctionExpr {
                        function,
                        inputs: vec![
                            self.plan_scalar(table_refs, &expr.left)?,
                            self.plan_scalar(table_refs, &expr.right)?,
                        ],
                    },
                ))
            }
            Expression::Conjunction(expr) => {
                let scalar = expr.op.as_scalar_function();
                let function =
                    scalar.plan_from_expressions(self.bind_context, &[&expr.left, &expr.right])?;

                Ok(PhysicalScalarExpression::ScalarFunction(
                    PhysicalScalarFunctionExpr {
                        function,
                        inputs: vec![
                            self.plan_scalar(table_refs, &expr.left)?,
                            self.plan_scalar(table_refs, &expr.right)?,
                        ],
                    },
                ))
            }
            Expression::Arith(expr) => {
                let scalar = expr.op.as_scalar_function();
                let function =
                    scalar.plan_from_expressions(self.bind_context, &[&expr.left, &expr.right])?;

                Ok(PhysicalScalarExpression::ScalarFunction(
                    PhysicalScalarFunctionExpr {
                        function,
                        inputs: vec![
                            self.plan_scalar(table_refs, &expr.left)?,
                            self.plan_scalar(table_refs, &expr.right)?,
                        ],
                    },
                ))
            }
            other => Err(RayexecError::new(format!(
                "Unsupported scalar expression: {other}"
            ))),
        }
    }

    /// Plans join conditions by ANDind all conditions to produce a single
    /// physical expression.
    pub fn plan_join_conditions(
        &self,
        table_refs: &[TableRef],
        conditions: &[ComparisonCondition],
    ) -> Result<PhysicalScalarExpression> {
        unimplemented!()
        // conditions
        //     .iter()
        //     .map(|c| self.plan_join_condition(table_refs, c))
        //     .collect::<Result<Vec<_>>>()
    }

    pub fn plan_join_condition(
        &self,
        table_refs: &[TableRef],
        condition: &ComparisonCondition,
    ) -> Result<PhysicalScalarExpression> {
        let scalar = condition.op.as_scalar_function();
        let function = scalar
            .plan_from_expressions(self.bind_context, &[&condition.left, &condition.right])?;

        Ok(PhysicalScalarExpression::ScalarFunction(
            PhysicalScalarFunctionExpr {
                function,
                inputs: vec![
                    self.plan_scalar(table_refs, &condition.left)?,
                    self.plan_scalar(table_refs, &condition.right)?,
                ],
            },
        ))
    }

    pub fn plan_sorts(
        &self,
        table_refs: &[TableRef],
        exprs: &[BoundOrderByExpr],
    ) -> Result<Vec<PhysicalSortExpression>> {
        exprs
            .iter()
            .map(|expr| self.plan_sort(table_refs, expr))
            .collect::<Result<Vec<_>>>()
    }

    /// Plan a sort expression.
    ///
    /// Sort expressions should be column expressions pointing to some
    /// pre-projection.
    pub fn plan_sort(
        &self,
        table_refs: &[TableRef],
        expr: &BoundOrderByExpr,
    ) -> Result<PhysicalSortExpression> {
        let scalar = self.plan_scalar(table_refs, &expr.expr)?;
        match scalar {
            PhysicalScalarExpression::Column(column) => Ok(PhysicalSortExpression {
                column,
                desc: expr.desc,
                nulls_first: expr.nulls_first,
            }),
            other => Err(RayexecError::new(format!(
                "Expected column expression for sort expression, got: {other}"
            ))),
        }
    }
}
