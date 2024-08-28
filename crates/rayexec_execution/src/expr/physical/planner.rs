use crate::{
    expr::{physical::PhysicalScalarExpression, Expression},
    logical::binder::bind_context::{BindContext, TableRef},
};
use fmtutil::IntoDisplayableSlice;
use rayexec_error::{not_implemented, RayexecError, Result};

use super::{
    cast_expr::PhysicalCastExpr, column_expr::PhysicalColumnExpr,
    literal_expr::PhysicalLiteralExpr, scalar_function_expr::PhysicalScalarFunctionExpr,
    PhysicalAggregateExpression,
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
                        table_refs.displayable(),
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
                let scalar = expr.op.scalar_function();
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
                let scalar = expr.conjunction.scalar_function();
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

    /// Plan aggregate expressions.
    ///
    /// All aggregate input expression must reference a single pre-projection
    /// table containing the true input expressions.
    pub fn plan_aggregates(
        &self,
        table_ref: TableRef,
        aggregates: &[Expression],
    ) -> Result<Vec<PhysicalAggregateExpression>> {
        aggregates
            .iter()
            .map(|agg| self.plan_aggregate(table_ref, agg))
            .collect::<Result<Vec<_>>>()
    }

    fn plan_aggregate(
        &self,
        table_ref: TableRef,
        aggregate: &Expression,
    ) -> Result<PhysicalAggregateExpression> {
        match aggregate {
            Expression::Aggregate(agg) => {
                if agg.filter.is_some() {
                    not_implemented!("aggregate filter");
                }

                let columns = agg
                    .inputs
                    .iter()
                    .map(|expr| match self.plan_scalar(&[table_ref], expr)? {
                        PhysicalScalarExpression::Column(col) => Ok(col),
                        other => {
                            return Err(RayexecError::new(format!(
                            "Expected column expression for physical aggregate input, got: {other}"
                        )))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(PhysicalAggregateExpression {
                    function: agg.agg.clone(),
                    columns,
                    output_type: agg.datatype(self.bind_context)?,
                })
            }
            other => Err(RayexecError::new(format!(
                "Expected aggregate expression, got: {other}"
            ))),
        }
    }
}