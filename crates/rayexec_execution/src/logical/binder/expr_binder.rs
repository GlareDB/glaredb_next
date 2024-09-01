use fmtutil::IntoDisplayableSlice;
use rayexec_bullet::{datatype::DataType, scalar::OwnedScalarValue};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use rayexec_parser::ast;

use crate::{
    expr::{
        aggregate_expr::AggregateExpr,
        arith_expr::{ArithExpr, ArithOperator},
        cast_expr::CastExpr,
        column_expr::ColumnExpr,
        comparison_expr::{ComparisonExpr, ComparisonOperator},
        literal_expr::LiteralExpr,
        scalar_function_expr::ScalarFunctionExpr,
        AsScalarFunction, Expression,
    },
    functions::{
        aggregate::AggregateFunction,
        scalar::{
            list::{ListExtract, ListValues},
            ScalarFunction,
        },
        CastType,
    },
    logical::{
        binder::bind_context::CorrelatedColumn,
        resolver::{
            resolve_context::ResolveContext, resolved_function::ResolvedFunction, ResolvedMeta,
        },
    },
};

use super::bind_context::{BindContext, BindScopeRef, TableAlias, TableRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecursionContext {
    pub allow_aggregate: bool,
    pub allow_window: bool,
}

#[derive(Debug)]
pub struct ExpressionBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ExpressionBinder<'a> {
    pub const fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        ExpressionBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_expressions(
        &self,
        bind_context: &mut BindContext,
        exprs: &[ast::Expr<ResolvedMeta>],
        recur: RecursionContext,
    ) -> Result<Vec<Expression>> {
        exprs
            .iter()
            .map(|expr| self.bind_expression(bind_context, expr, recur))
            .collect::<Result<Vec<_>>>()
    }

    pub fn bind_expression(
        &self,
        bind_context: &mut BindContext,
        expr: &ast::Expr<ResolvedMeta>,
        recur: RecursionContext,
    ) -> Result<Expression> {
        match expr {
            ast::Expr::Ident(ident) => self.bind_ident(bind_context, ident),
            ast::Expr::CompoundIdent(idents) => self.bind_idents(bind_context, idents),
            ast::Expr::Literal(literal) => Self::bind_literal(literal),
            ast::Expr::Array(arr) => {
                let exprs = arr
                    .into_iter()
                    .map(|v| self.bind_expression(bind_context, v, recur))
                    .collect::<Result<Vec<_>>>()?;

                let scalar = Box::new(ListValues);
                let exprs =
                    self.apply_casts_for_scalar_function(bind_context, scalar.as_ref(), exprs)?;

                let refs: Vec<_> = exprs.iter().collect();
                let planned = scalar.plan_from_expressions(bind_context, &refs)?;

                Ok(Expression::ScalarFunction(ScalarFunctionExpr {
                    function: planned,
                    inputs: exprs,
                }))
            }
            ast::Expr::ArraySubscript { expr, subscript } => {
                let expr = self.bind_expression(bind_context, expr.as_ref(), recur)?;
                match subscript.as_ref() {
                    ast::ArraySubscript::Index(index) => {
                        let index = self.bind_expression(
                            bind_context,
                            index,
                            RecursionContext {
                                allow_window: false,
                                allow_aggregate: false,
                            },
                        )?;

                        let scalar = Box::new(ListExtract);
                        let mut exprs = self.apply_casts_for_scalar_function(
                            bind_context,
                            scalar.as_ref(),
                            vec![expr, index],
                        )?;
                        let index = exprs.pop().unwrap();
                        let expr = exprs.pop().unwrap();

                        let planned =
                            scalar.plan_from_expressions(bind_context, &[&expr, &index])?;

                        Ok(Expression::ScalarFunction(ScalarFunctionExpr {
                            function: planned,
                            inputs: vec![expr, index],
                        }))
                    }
                    ast::ArraySubscript::Slice { .. } => {
                        Err(RayexecError::new("Array slicing not yet implemented"))
                    }
                }
            }
            ast::Expr::UnaryExpr { .. } => {
                unimplemented!()
            }
            ast::Expr::BinaryExpr { left, op, right } => {
                let left = self.bind_expression(bind_context, left, recur)?;
                let right = self.bind_expression(bind_context, right, recur)?;

                Ok(match op {
                    ast::BinaryOperator::NotEq => {
                        let op = ComparisonOperator::NotEq;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Comparison(ComparisonExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::Eq => {
                        let op = ComparisonOperator::Eq;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Comparison(ComparisonExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::Lt => {
                        let op = ComparisonOperator::Lt;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Comparison(ComparisonExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::LtEq => {
                        let op = ComparisonOperator::LtEq;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Comparison(ComparisonExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::Gt => {
                        let op = ComparisonOperator::Gt;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Comparison(ComparisonExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::GtEq => {
                        let op = ComparisonOperator::GtEq;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Comparison(ComparisonExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::Plus => {
                        let op = ArithOperator::Add;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Arith(ArithExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::Minus => {
                        let op = ArithOperator::Sub;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Arith(ArithExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::Multiply => {
                        let op = ArithOperator::Mul;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Arith(ArithExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::Divide => {
                        let op = ArithOperator::Div;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Arith(ArithExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    ast::BinaryOperator::Modulo => {
                        let op = ArithOperator::Mod;
                        let [left, right] =
                            self.apply_cast_for_operator(bind_context, op, [left, right])?;
                        Expression::Arith(ArithExpr {
                            left: Box::new(left),
                            right: Box::new(right),
                            op,
                        })
                    }
                    other => not_implemented!("binary operator {other:?}"),
                })
            }
            ast::Expr::Function(func) => {
                let reference = self
                    .resolve_context
                    .functions
                    .try_get_bound(func.reference)?;

                let recur = if reference.0.is_aggregate() {
                    RecursionContext {
                        allow_window: false,
                        allow_aggregate: false,
                    }
                } else {
                    recur
                };

                let inputs = func
                    .args
                    .iter()
                    .map(|arg| match arg {
                        ast::FunctionArg::Unnamed { arg } => match arg {
                            ast::FunctionArgExpr::Expr(expr) => {
                                Ok(self.bind_expression(bind_context, &expr, recur)?)
                            }
                            ast::FunctionArgExpr::Wildcard => {
                                // Resolver should have handled removing '*'
                                // from function calls.
                                Err(RayexecError::new(
                                    "Cannot plan a function with '*' as an argument",
                                ))
                            }
                        },
                        ast::FunctionArg::Named { .. } => Err(RayexecError::new(
                            "Named arguments to scalar functions not supported",
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;

                // TODO: This should probably assert that location == any since
                // I don't think it makes sense to try to handle different sets
                // of scalar/aggs in the hybrid case yet.
                match reference {
                    (ResolvedFunction::Scalar(scalar), _) => {
                        println!("INPUTS: {inputs:?}");

                        let inputs = self.apply_casts_for_scalar_function(
                            bind_context,
                            scalar.as_ref(),
                            inputs,
                        )?;

                        println!("CASTS: {inputs:?}");

                        let refs: Vec<_> = inputs.iter().collect();
                        let function = scalar.plan_from_expressions(bind_context, &refs)?;

                        Ok(Expression::ScalarFunction(ScalarFunctionExpr {
                            function,
                            inputs,
                        }))
                    }
                    (ResolvedFunction::Aggregate(agg), _) => {
                        let inputs = self.apply_casts_for_aggregate_function(
                            bind_context,
                            agg.as_ref(),
                            inputs,
                        )?;

                        let refs: Vec<_> = inputs.iter().collect();
                        let agg = agg.plan_from_expressions(bind_context, &refs)?;

                        Ok(Expression::Aggregate(AggregateExpr {
                            agg,
                            inputs,
                            filter: None,
                        }))
                    }
                }
            }
            other => unimplemented!("{other:?}"),
        }
    }

    pub(crate) fn bind_literal(literal: &ast::Literal<ResolvedMeta>) -> Result<Expression> {
        Ok(match literal {
            ast::Literal::Number(n) => {
                if let Ok(n) = n.parse::<i64>() {
                    Expression::Literal(LiteralExpr {
                        literal: OwnedScalarValue::Int64(n),
                    })
                } else if let Ok(n) = n.parse::<u64>() {
                    Expression::Literal(LiteralExpr {
                        literal: OwnedScalarValue::UInt64(n),
                    })
                } else if let Ok(n) = n.parse::<f64>() {
                    Expression::Literal(LiteralExpr {
                        literal: OwnedScalarValue::Float64(n),
                    })
                } else {
                    return Err(RayexecError::new(format!(
                        "Unable to parse {n} as a number"
                    )));
                }
            }
            ast::Literal::Boolean(b) => Expression::Literal(LiteralExpr {
                literal: OwnedScalarValue::Boolean(*b),
            }),
            ast::Literal::Null => Expression::Literal(LiteralExpr {
                literal: OwnedScalarValue::Null,
            }),
            ast::Literal::SingleQuotedString(s) => Expression::Literal(LiteralExpr {
                literal: OwnedScalarValue::Utf8(s.to_string().into()),
            }),
            other => {
                return Err(RayexecError::new(format!(
                    "Unusupported SQL literal: {other:?}"
                )))
            }
        })
    }

    fn bind_column(
        &self,
        bind_context: &mut BindContext,
        alias: Option<TableAlias>,
        col: &str,
    ) -> Result<Expression> {
        let mut current = self.current;
        loop {
            let table = bind_context.find_table_for_column(current, alias.as_ref(), &col)?;
            match table {
                Some((table, col_idx)) => {
                    let table = table.reference;

                    // Table containing column found. Check if it's correlated
                    // (referencing an outer context).
                    let is_correlated = current != self.current;

                    if is_correlated {
                        // Column is correlated, Push correlation to current
                        // bind context.
                        let correlated = CorrelatedColumn {
                            outer: current,
                            table,
                            col_idx,
                        };

                        // Note `self.current`, not `current`. We want to store
                        // the context containing the expression.
                        bind_context.push_correlation(self.current, correlated)?;
                    }

                    return Ok(Expression::Column(ColumnExpr {
                        table_scope: table,
                        column: col_idx,
                    }));
                }
                None => {
                    // Table not found in current context, go to parent context
                    // relative the context we just searched.
                    match bind_context.get_parent_ref(current)? {
                        Some(parent) => current = parent,
                        None => {
                            // We're at root, no column with this ident in query.
                            return Err(RayexecError::new(format!(
                                "Missing column for reference: {col}",
                            )));
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn bind_ident(
        &self,
        bind_context: &mut BindContext,
        ident: &ast::Ident,
    ) -> Result<Expression> {
        let col = ident.as_normalized_string();
        self.bind_column(bind_context, None, &col)
    }

    /// Plan a compound identifier.
    ///
    /// Assumed to be a reference to a column either in the current scope or one
    /// of the outer scopes.
    fn bind_idents(
        &self,
        bind_context: &mut BindContext,
        idents: &[ast::Ident],
    ) -> Result<Expression> {
        match idents.len() {
            0 => Err(RayexecError::new("Empty identifier")),
            1 => {
                // Single column.
                self.bind_ident(bind_context, &idents[0])
            }
            2..=4 => {
                // Qualified column.
                // 2 => 'table.column'
                // 3 => 'schema.table.column'
                // 4 => 'database.schema.table.column'
                // TODO: Struct fields.

                let mut idents = idents.to_vec();

                let col = idents.pop().unwrap().into_normalized_string();

                let alias = TableAlias {
                    table: idents
                        .pop()
                        .map(|ident| ident.into_normalized_string())
                        .unwrap(), // Must exist
                    schema: idents.pop().map(|ident| ident.into_normalized_string()), // May exist
                    database: idents.pop().map(|ident| ident.into_normalized_string()), // May exist
                };

                self.bind_column(bind_context, Some(alias), &col)
            }
            _ => Err(RayexecError::new(format!(
                "Too many identifier parts in {}",
                ast::ObjectReference(idents.to_vec()),
            ))), // TODO: Struct fields.
        }
    }

    fn apply_cast_for_operator<const N: usize>(
        &self,
        bind_context: &BindContext,
        operator: impl AsScalarFunction,
        inputs: [Expression; N],
    ) -> Result<[Expression; N]> {
        let inputs = self.apply_casts_for_scalar_function(
            bind_context,
            operator.as_scalar_function(),
            inputs.to_vec(),
        )?;
        inputs
            .try_into()
            .map_err(|_| RayexecError::new("Number of casted inputs incorrect"))
    }

    /// Applies casts to an input expression based on the signatures for a
    /// scalar function.
    fn apply_casts_for_scalar_function(
        &self,
        bind_context: &BindContext,
        scalar: &dyn ScalarFunction,
        inputs: Vec<Expression>,
    ) -> Result<Vec<Expression>> {
        let input_datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        if scalar.exact_signature(&input_datatypes).is_some() {
            // Exact
            Ok(inputs)
        } else {
            // Try to find candidates that we can cast to.
            let mut candidates = scalar.candidate(&input_datatypes);

            if candidates.is_empty() {
                // TODO: Do we want to fall through? Is it possible for a
                // scalar and aggregate function to have the same name?

                // TODO: Better error.
                return Err(RayexecError::new(format!(
                    "Invalid inputs to '{}': {}",
                    scalar.name(),
                    input_datatypes.display_with_brackets(),
                )));
            }

            // TODO: Maybe more sophisticated candidate selection.
            //
            // TODO: Sort by score
            //
            // We should do some lightweight const folding and prefer candidates
            // that cast the consts over ones that need array inputs to be
            // casted.
            let candidate = candidates.swap_remove(0);

            // Apply casts where needed.
            let inputs = inputs
                .into_iter()
                .zip(candidate.casts)
                .map(|(input, cast_to)| {
                    Ok(match cast_to {
                        CastType::Cast { to, .. } => Expression::Cast(CastExpr {
                            to: DataType::try_default_datatype(to)?,
                            expr: Box::new(input),
                        }),
                        CastType::NoCastNeeded => input,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(inputs)
        }
    }

    // TODO: Reduce dupliation with the scalar one.
    fn apply_casts_for_aggregate_function(
        &self,
        bind_context: &BindContext,
        agg: &dyn AggregateFunction,
        inputs: Vec<Expression>,
    ) -> Result<Vec<Expression>> {
        let input_datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        if agg.exact_signature(&input_datatypes).is_some() {
            // Exact
            Ok(inputs)
        } else {
            // Try to find candidates that we can cast to.
            let mut candidates = agg.candidate(&input_datatypes);

            if candidates.is_empty() {
                return Err(RayexecError::new(format!(
                    "Invalid inputs to '{}': {}",
                    agg.name(),
                    input_datatypes.display_with_brackets(),
                )));
            }

            // TODO: Maybe more sophisticated candidate selection.
            let candidate = candidates.swap_remove(0);

            // Apply casts where needed.
            let inputs = inputs
                .into_iter()
                .zip(candidate.casts)
                .map(|(input, cast_to)| {
                    Ok(match cast_to {
                        CastType::Cast { to, .. } => Expression::Cast(CastExpr {
                            to: DataType::try_default_datatype(to)?,
                            expr: Box::new(input),
                        }),
                        CastType::NoCastNeeded => input,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(inputs)
        }
    }
}
