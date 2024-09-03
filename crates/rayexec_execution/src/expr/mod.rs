pub mod aggregate_expr;
pub mod arith_expr;
pub mod between_expr;
pub mod case_expr;
pub mod cast_expr;
pub mod column_expr;
pub mod comparison_expr;
pub mod conjunction_expr;
pub mod is_expr;
pub mod literal_expr;
pub mod negate_expr;
pub mod scalar;
pub mod scalar_function_expr;
pub mod subquery_expr;
pub mod window_expr;

pub mod physical;

use crate::database::DatabaseContext;
use crate::functions::aggregate::PlannedAggregateFunction;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::expr::LogicalExpression;
use crate::proto::DatabaseProtoConv;
use aggregate_expr::AggregateExpr;
use arith_expr::ArithExpr;
use between_expr::BetweenExpr;
use case_expr::CaseExpr;
use cast_expr::CastExpr;
use column_expr::ColumnExpr;
use comparison_expr::ComparisonExpr;
use conjunction_expr::{ConjunctionExpr, ConjunctionOperator};
use fmtutil::IntoDisplayableSlice;
use is_expr::IsExpr;
use literal_expr::LiteralExpr;
use negate_expr::NegateExpr;
use rayexec_bullet::compute::cast::array::cast_array;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::TypeSchema;
use rayexec_bullet::{array::Array, batch::Batch, scalar::OwnedScalarValue};
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};
use rayexec_proto::ProtoConv;
use scalar_function_expr::ScalarFunctionExpr;
use std::fmt::{self, Debug};
use std::sync::Arc;
use subquery_expr::SubqueryExpr;
use window_expr::WindowExpr;

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Aggregate(AggregateExpr),
    Arith(ArithExpr),
    Between(BetweenExpr),
    Case(CaseExpr),
    Cast(CastExpr),
    Column(ColumnExpr),
    Comparison(ComparisonExpr),
    Conjunction(ConjunctionExpr),
    Is(IsExpr),
    Literal(LiteralExpr),
    Negate(NegateExpr),
    ScalarFunction(ScalarFunctionExpr),
    Subquery(SubqueryExpr),
    Window(WindowExpr),
}

impl Expression {
    pub fn datatype(&self, bind_context: &BindContext) -> Result<DataType> {
        Ok(match self {
            Self::Aggregate(expr) => expr.agg.return_type(),
            Self::Arith(expr) => {
                let func = expr
                    .op
                    .as_scalar_function()
                    .plan_from_expressions(bind_context, &[&expr.left, &expr.right])?;
                func.return_type()
            }
            Self::Between(_) => DataType::Boolean,
            Self::Case(expr) => expr.datatype(bind_context)?,
            Self::Cast(expr) => expr.to.clone(),
            Self::Column(expr) => expr.datatype(bind_context)?,
            Self::Comparison(_) => DataType::Boolean,
            Self::Conjunction(_) => DataType::Boolean,
            Self::Is(_) => DataType::Boolean,
            Self::Literal(expr) => expr.literal.datatype(),
            Self::Negate(expr) => expr.datatype(bind_context)?,
            Self::ScalarFunction(expr) => expr.function.return_type(),
            Self::Subquery(_) => unimplemented!(),
            Self::Window(_) => not_implemented!("WINDOW"),
        })
    }

    /// ANDs all expressions, only returning None if iterator contains no
    /// expressions.
    pub fn and_all(exprs: impl IntoIterator<Item = Expression>) -> Option<Expression> {
        let mut exprs = exprs.into_iter();
        let left = exprs.next()?;

        Some(exprs.fold(left, |left, right| {
            Expression::Conjunction(ConjunctionExpr {
                left: Box::new(left),
                right: Box::new(right),
                op: ConjunctionOperator::And,
            })
        }))
    }

    pub fn for_each_child_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        match self {
            Self::Aggregate(agg) => {
                for expr in &mut agg.inputs {
                    func(expr)?;
                }
                if let Some(filter) = agg.filter.as_mut() {
                    func(filter)?;
                }
            }
            Self::Arith(arith) => {
                func(&mut arith.left)?;
                func(&mut arith.right)?;
            }
            Self::Between(between) => {
                func(&mut between.lower)?;
                func(&mut between.upper)?;
                func(&mut between.input)?;
            }
            Self::Cast(cast) => {
                func(&mut cast.expr)?;
            }
            Self::Case(case) => {
                for when_then in &mut case.cases {
                    func(&mut when_then.when)?;
                    func(&mut when_then.then)?;
                }
                if let Some(else_expr) = case.else_expr.as_mut() {
                    func(else_expr)?;
                }
            }
            Self::Column(_) => (),
            Self::Comparison(comp) => {
                func(&mut comp.left)?;
                func(&mut comp.right)?;
            }
            Self::Conjunction(conj) => {
                func(&mut conj.left)?;
                func(&mut conj.right)?;
            }
            Self::Is(is) => func(&mut is.input)?,
            Self::Literal(_) => (),
            Self::Negate(negate) => func(&mut negate.expr)?,
            Self::ScalarFunction(scalar) => {
                for input in &mut scalar.inputs {
                    func(input)?;
                }
            }
            Self::Subquery(_) => (),
            Self::Window(window) => {
                for input in &mut window.inputs {
                    func(input)?;
                }
                func(&mut window.filter)?;
                for partition in &mut window.partition_by {
                    func(partition)?;
                }
            }
        }
        Ok(())
    }

    pub fn for_each_child<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        match self {
            Self::Aggregate(agg) => {
                for expr in &agg.inputs {
                    func(expr)?;
                }
                if let Some(filter) = agg.filter.as_ref() {
                    func(filter)?;
                }
            }
            Self::Arith(arith) => {
                func(&arith.left)?;
                func(&arith.right)?;
            }
            Self::Between(between) => {
                func(&between.lower)?;
                func(&between.upper)?;
                func(&between.input)?;
            }
            Self::Cast(cast) => {
                func(&cast.expr)?;
            }
            Self::Case(case) => {
                for when_then in &case.cases {
                    func(&when_then.when)?;
                    func(&when_then.then)?;
                }
                if let Some(else_expr) = case.else_expr.as_ref() {
                    func(else_expr)?;
                }
            }
            Self::Column(_) => (),
            Self::Comparison(comp) => {
                func(&comp.left)?;
                func(&comp.right)?;
            }
            Self::Conjunction(conj) => {
                func(&conj.left)?;
                func(&conj.right)?;
            }
            Self::Is(is) => func(&is.input)?,
            Self::Literal(_) => (),
            Self::Negate(negate) => func(&negate.expr)?,
            Self::ScalarFunction(scalar) => {
                for input in &scalar.inputs {
                    func(input)?;
                }
            }
            Self::Subquery(_) => (),
            Self::Window(window) => {
                for input in &window.inputs {
                    func(input)?;
                }
                func(&window.filter)?;
                for partition in &window.partition_by {
                    func(partition)?;
                }
            }
        }
        Ok(())
    }

    pub fn contains_subquery(&self) -> bool {
        match self {
            Self::Subquery(_) => true,
            _ => {
                let mut has_subquery = false;
                self.for_each_child(&mut |expr| {
                    has_subquery = has_subquery || expr.contains_subquery();
                    Ok(())
                })
                .expect("subquery check to no fail");
                has_subquery
            }
        }
    }

    pub fn is_constant(&self) -> bool {
        unimplemented!()
    }

    pub const fn is_column_expr(&self) -> bool {
        matches!(self, Self::Column(_))
    }

    /// Try to get a top-level literal from this expression, erroring if it's
    /// not one.
    pub fn try_into_scalar(self) -> Result<OwnedScalarValue> {
        match self {
            Self::Literal(lit) => Ok(lit.literal),
            other => Err(RayexecError::new(format!("Not a literal: {other:?}"))),
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aggregate(expr) => write!(f, "{}", expr),
            Self::Arith(expr) => write!(f, "{}", expr),
            Self::Between(expr) => write!(f, "{}", expr),
            Self::Case(expr) => write!(f, "{}", expr),
            Self::Cast(expr) => write!(f, "{}", expr),
            Self::Column(expr) => write!(f, "{}", expr),
            Self::Comparison(expr) => write!(f, "{}", expr),
            Self::Conjunction(expr) => write!(f, "{}", expr),
            Self::Is(expr) => write!(f, "{}", expr),
            Self::Literal(expr) => write!(f, "{}", expr),
            Self::Negate(expr) => write!(f, "{}", expr),
            Self::ScalarFunction(expr) => write!(f, "{}", expr),
            Self::Subquery(expr) => write!(f, "{}", expr),
            Self::Window(expr) => write!(f, "{}", expr),
        }
    }
}

pub trait AsScalarFunction {
    /// Returns the scalar function that implements the expression.
    fn as_scalar_function(&self) -> &dyn ScalarFunction;
}

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
        function: Box<dyn PlannedScalarFunction>,

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
                        input.types.len() as i64 - 1 // Cast to i64 in case input we pass in has 0 columns.
                    )));
                }
                PhysicalScalarExpression::Column(col)
            }
            LogicalExpression::Literal(lit) => PhysicalScalarExpression::Literal(lit),
            LogicalExpression::Unary { op, expr } => {
                let input = PhysicalScalarExpression::try_from_uncorrelated_expr(*expr, input)?;

                PhysicalScalarExpression::ScalarFunction {
                    function: op.scalar,
                    inputs: vec![input],
                }
            }
            LogicalExpression::Binary { op, left, right } => {
                let left = PhysicalScalarExpression::try_from_uncorrelated_expr(*left, input)?;
                let right = PhysicalScalarExpression::try_from_uncorrelated_expr(*right, input)?;

                PhysicalScalarExpression::ScalarFunction {
                    function: op.scalar,
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
                let inputs = inputs
                    .into_iter()
                    .map(|expr| PhysicalScalarExpression::try_from_uncorrelated_expr(expr, input))
                    .collect::<Result<Vec<_>>>()?;

                PhysicalScalarExpression::ScalarFunction { function, inputs }
            }
            LogicalExpression::Subquery(_) => {
                // Should have already been taken care of during planning.
                return Err(RayexecError::new(
                    "Cannot convert a subquery into a physical expression",
                ));
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
                let mut out = function.execute(&refs)?;

                // If function is provided no input, it's expected to return an
                // array of length 1. We extend the array here so that it's the
                // same size as the rest.
                if refs.is_empty() {
                    let scalar = out
                        .scalar(0)
                        .ok_or_else(|| RayexecError::new("Missing scalar at index 0"))?;

                    // TODO: Probably want to check null, and create the
                    // appropriate array type since this will create a
                    // NullArray, and not the type we're expecting.
                    out = scalar.as_array(batch.num_rows());
                }

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

impl DatabaseProtoConv for PhysicalScalarExpression {
    type ProtoType = rayexec_proto::generated::expr::PhysicalScalarExpression;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::expr::{
            physical_scalar_expression::Value, PhysicalCastExpression, PhysicalScalarFunction,
        };

        let value = match self {
            Self::Column(idx) => Value::Column(*idx as u32),
            Self::Literal(v) => Value::Literal(v.to_proto()?),
            Self::Cast { to, expr } => Value::Cast(Box::new(PhysicalCastExpression {
                cast_to: Some(to.to_proto()?),
                expr: Some(Box::new(expr.to_proto_ctx(context)?)),
            })),
            Self::ScalarFunction { function, inputs } => {
                Value::ScalarFunction(PhysicalScalarFunction {
                    function: Some(function.to_proto_ctx(context)?),
                    inputs: inputs
                        .iter()
                        .map(|input| input.to_proto_ctx(context))
                        .collect::<Result<Vec<_>>>()?,
                })
            }
            Self::Case { .. } => unimplemented!(),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::expr::physical_scalar_expression::Value;

        Ok(match proto.value.required("value")? {
            Value::Column(idx) => Self::Column(idx as usize),
            Value::Literal(v) => Self::Literal(OwnedScalarValue::from_proto(v)?),
            Value::Cast(cast) => Self::Cast {
                to: DataType::from_proto(cast.cast_to.required("cast_to")?)?,
                expr: Box::new(Self::from_proto_ctx(*cast.expr.required("expr")?, context)?),
            },
            Value::ScalarFunction(function) => {
                let inputs = function
                    .inputs
                    .into_iter()
                    .map(|input| Self::from_proto_ctx(input, context))
                    .collect::<Result<Vec<_>>>()?;
                let function = DatabaseProtoConv::from_proto_ctx(
                    function.function.required("function")?,
                    context,
                )?;
                Self::ScalarFunction { function, inputs }
            }
        })
    }
}

#[derive(Debug)]
pub struct PhysicalAggregateExpression {
    /// The function we'll be calling to produce the aggregate states.
    pub function: Box<dyn PlannedAggregateFunction>,

    /// Column indices for the input we'll be aggregating on.
    pub column_indices: Vec<usize>,

    /// Output type of the aggregate.
    pub output_type: DataType,
    // TODO: Filter
}

impl PhysicalAggregateExpression {
    pub fn try_from_logical_expression(
        expr: LogicalExpression,
        _input: &TypeSchema, // TODO: Do wil still need this?
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

                let output_type = agg.return_type();

                PhysicalAggregateExpression {
                    function: agg,
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
            self.column_indices.display_with_brackets(),
        )
    }
}

impl DatabaseProtoConv for PhysicalAggregateExpression {
    type ProtoType = rayexec_proto::generated::expr::PhysicalAggregateExpression;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            function: Some(self.function.to_proto_ctx(context)?),
            column_indices: self.column_indices.iter().map(|c| *c as u64).collect(),
            output_type: Some(self.output_type.to_proto()?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            function: DatabaseProtoConv::from_proto_ctx(
                proto.function.required("function")?,
                context,
            )?,
            column_indices: proto
                .column_indices
                .into_iter()
                .map(|i| i as usize)
                .collect(),
            output_type: DataType::from_proto(proto.output_type.required("datatype")?)?,
        })
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

impl ProtoConv for PhysicalSortExpression {
    type ProtoType = rayexec_proto::generated::expr::PhysicalSortExpression;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            column: self.column as u64,
            desc: self.desc,
            nulls_first: self.nulls_first,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            column: proto.column as usize,
            desc: proto.desc,
            nulls_first: proto.nulls_first,
        })
    }
}
