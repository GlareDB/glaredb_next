pub mod scalar;

use crate::functions::aggregate::SpecializedAggregateFunction;
use crate::functions::scalar::SpecializedScalarFunction;
use crate::logical::operator::LogicalExpression;
use rayexec_bullet::compute::cast::array::cast_array;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::TypeSchema;
use rayexec_bullet::{array::Array, batch::Batch, scalar::OwnedScalarValue};
use rayexec_error::{RayexecError, Result};
use std::fmt::{self, Debug};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum PhysicalScalarExpression {
    /// Reference to a column in the input batch.
    Column(usize),

    /// A scalar literal.
    Literal(OwnedScalarValue),

    /// Cast an input expression.
    Cast {
        to: DataType,
        expr: Box<PhysicalScalarExpression>,
    },

    /// A scalar function.
    ScalarFunction {
        /// The specialized function we'll be calling.
        function: Box<dyn SpecializedScalarFunction>,

        /// Column inputs into the function.
        inputs: Vec<PhysicalScalarExpression>,
    },

    /// Case expressions.
    Case {
        input: Box<PhysicalScalarExpression>,
        /// When <left>, then <right>
        when_then: Vec<(PhysicalScalarExpression, PhysicalScalarExpression)>,
    },
}

impl PhysicalScalarExpression {
    /// Try to produce a physical expression from a logical expression.
    ///
    /// Errors if the expression is not scalar, or if it contains correlated
    /// columns (columns that reference an outer scope).
    pub fn try_from_uncorrelated_expr(
        logical: LogicalExpression,
        input: &TypeSchema,
    ) -> Result<Self> {
        Ok(match logical {
            LogicalExpression::ColumnRef(col) => {
                let col = col.try_as_uncorrelated()?;
                if col >= input.types.len() {
                    return Err(RayexecError::new(format!(
                        "Invalid column index '{}', max index: '{}'",
                        col,
                        input.types.len() - 1
                    )));
                }
                PhysicalScalarExpression::Column(col)
            }
            LogicalExpression::Literal(lit) => PhysicalScalarExpression::Literal(lit),
            LogicalExpression::Unary { op, expr } => {
                let datatype = expr.datatype(input, &[])?;
                let input = PhysicalScalarExpression::try_from_uncorrelated_expr(*expr, input)?;

                let func = op.scalar_function();
                let specialized = func.specialize(&[datatype])?;

                PhysicalScalarExpression::ScalarFunction {
                    function: specialized,
                    inputs: vec![input],
                }
            }
            LogicalExpression::Binary { op, left, right } => {
                let left_datatype = left.datatype(input, &[])?;
                let right_datatype = right.datatype(input, &[])?;

                let left = PhysicalScalarExpression::try_from_uncorrelated_expr(*left, input)?;
                let right = PhysicalScalarExpression::try_from_uncorrelated_expr(*right, input)?;

                let scalar_inputs = &[left_datatype, right_datatype];
                let func = op.scalar_function();
                let specialized = func.specialize(scalar_inputs)?;

                PhysicalScalarExpression::ScalarFunction {
                    function: specialized,
                    inputs: vec![left, right],
                }
            }
            LogicalExpression::Cast { to, expr } => PhysicalScalarExpression::Cast {
                to,
                expr: Box::new(PhysicalScalarExpression::try_from_uncorrelated_expr(
                    *expr, input,
                )?),
            },
            LogicalExpression::ScalarFunction { function, inputs } => {
                let datatypes = inputs
                    .iter()
                    .map(|arg| arg.datatype(input, &[]))
                    .collect::<Result<Vec<_>>>()?;

                let inputs = inputs
                    .into_iter()
                    .map(|expr| PhysicalScalarExpression::try_from_uncorrelated_expr(expr, input))
                    .collect::<Result<Vec<_>>>()?;

                let specialized = function.specialize(&datatypes)?;

                PhysicalScalarExpression::ScalarFunction {
                    function: specialized,
                    inputs,
                }
            }
            other => unimplemented!("{other:?}"),
        })
    }

    /// Evaluate this expression on a batch.
    ///
    /// The number of elements in the resulting array will equal the number of
    /// rows in the input batch.
    pub fn eval(&self, batch: &Batch) -> Result<Arc<Array>> {
        Ok(match self {
            Self::Column(idx) => batch
                .column(*idx)
                .ok_or_else(|| {
                    RayexecError::new(format!(
                        "Tried to get column at index {} in a batch with {} columns",
                        idx,
                        batch.columns().len()
                    ))
                })?
                .clone(),
            Self::Literal(lit) => Arc::new(lit.as_array(batch.num_rows())),
            Self::ScalarFunction { function, inputs } => {
                let inputs = inputs
                    .iter()
                    .map(|input| input.eval(batch))
                    .collect::<Result<Vec<_>>>()?;
                let refs: Vec<_> = inputs.iter().collect(); // Can I not?
                let out = function.execute(&refs)?;

                // TODO: Do we want to Arc here? Should we allow batches to be mutable?

                Arc::new(out)
            }
            Self::Cast { to, expr } => {
                let input = expr.eval(batch)?;
                let out = cast_array(&input, to)?;
                Arc::new(out)
            }
            _ => unimplemented!(),
        })
    }
}

impl fmt::Display for PhysicalScalarExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(idx) => write!(f, "#{idx}"),
            Self::Literal(lit) => write!(f, "{lit}"),
            Self::Cast { to, expr } => write!(f, "cast({expr}, {to})"),
            Self::ScalarFunction { function, inputs } => write!(
                f,
                "{function:?}({})",
                inputs
                    .iter()
                    .map(|input| input.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Case { .. } => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub struct PhysicalAggregateExpression {
    /// The function we'll be calling to produce the aggregate states.
    pub function: Box<dyn SpecializedAggregateFunction>,

    /// Column indices for the input we'll be aggregating on.
    pub column_indices: Vec<usize>,

    /// Output type of the aggregate.
    pub output_type: DataType,
    // TODO: Filter
}

impl PhysicalAggregateExpression {
    pub fn try_from_logical_expression(
        expr: LogicalExpression,
        input: &TypeSchema,
    ) -> Result<Self> {
        Ok(match expr {
            LogicalExpression::Aggregate {
                agg,
                inputs,
                filter: _,
            } => {
                let column_indices = inputs.into_iter().map(|input| match input {
                    LogicalExpression::ColumnRef(col) => col.try_as_uncorrelated(),
                    other => Err(RayexecError::new(format!("Physical aggregate expressions must be constructed with uncorrelated column inputs, got: {other}"))),
                }).collect::<Result<Vec<_>>>()?;

                let input_types = column_indices.iter().map(|idx| input.types.get(*idx).cloned().ok_or_else(|| RayexecError::new(format!("Attempted to get a column outside the type schema, got: {idx}, max: {}", input.types.len() -1)))).collect::<Result<Vec<_>>>()?;
                let output_type = agg.return_type_for_inputs(&input_types).ok_or_else(|| RayexecError::new("Failed to get return type for aggregate while converting to physical expression"))?;

                let specialized = agg.specialize(&input_types)?;

                PhysicalAggregateExpression {
                    function: specialized,
                    column_indices,
                    output_type,
                }
            }
            other => {
                return Err(RayexecError::new(format!(
                "Cannot create a physical aggregate expression from logical expression: {other}",
            )))
            }
        })
    }
}

impl fmt::Display for PhysicalAggregateExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}({})",
            self.function,
            self.column_indices
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalSortExpression {
    /// Column this expression is for.
    pub column: usize,

    /// If sort should be descending.
    pub desc: bool,

    /// If nulls should be ordered first.
    pub nulls_first: bool,
}

impl PhysicalSortExpression {
    pub fn try_from_uncorrelated_expr(
        logical: LogicalExpression,
        input: &TypeSchema,
        desc: bool,
        nulls_first: bool,
    ) -> Result<Self> {
        match logical {
            LogicalExpression::ColumnRef(col) => {
                let col = col.try_as_uncorrelated()?;
                if col >= input.types.len() {
                    return Err(RayexecError::new(format!(
                        "Invalid column index '{}', max index: '{}'",
                        col,
                        input.types.len() - 1
                    )));
                }
                Ok(PhysicalSortExpression {
                    column: col,
                    desc,
                    nulls_first,
                })
            }
            other => Err(RayexecError::new(format!(
                "Cannot create a physical sort expression from logical expression: {other:?}"
            ))),
        }
    }
}

impl fmt::Display for PhysicalSortExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.column,
            if self.desc { "DESC" } else { "ASC" },
            if self.nulls_first {
                "NULLS FIRST"
            } else {
                "NULLS LAST"
            }
        )
    }
}
