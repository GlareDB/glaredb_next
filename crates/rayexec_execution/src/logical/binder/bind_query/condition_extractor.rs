use crate::{
    expr::{
        conjunction_expr::{ConjunctionExpr, ConjunctionOperator},
        Expression,
    },
    logical::{
        binder::bind_context::{BindContext, BindScopeRef},
        logical_join::ComparisonCondition,
    },
};
use rayexec_error::{not_implemented, RayexecError, Result};

#[derive(Debug)]
pub struct ExtractedConditions {
    /// Join conditions successfully extracted from expressions.
    pub comparisons: Vec<ComparisonCondition>,
    /// Expressions that we could not build a condition for.
    ///
    /// These expressions should filter the output of a join.
    pub arbitrary: Vec<Expression>,
    /// Expressions that only rely on inputs on the left side.
    ///
    /// These should be placed into a filter prior to the join.
    pub left_filter: Vec<Expression>,
    /// Expressions that only rely on inputs on the right side.
    ///
    /// These should be placed into a filter prior to the join.
    pub right_filter: Vec<Expression>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprJoinSide {
    Left,
    Right,
    Both,
    None,
}

impl ExprJoinSide {
    fn combine(self, other: Self) -> ExprJoinSide {
        match (self, other) {
            (a, Self::None) => a,
            (Self::None, b) => b,
            (Self::Both, _) => Self::Both,
            (_, Self::Both) => Self::Both,
            (Self::Left, Self::Left) => Self::Left,
            (Self::Right, Self::Right) => Self::Right,
            _ => Self::Both,
        }
    }
}

#[derive(Debug)]
pub struct JoinConditionExtractor<'a> {
    pub bind_context: &'a BindContext,
    pub left_scope: BindScopeRef,
    pub right_scope: BindScopeRef,
}

impl<'a> JoinConditionExtractor<'a> {
    pub fn new(
        bind_context: &'a BindContext,
        left_scope: BindScopeRef,
        right_scope: BindScopeRef,
    ) -> Self {
        JoinConditionExtractor {
            bind_context,
            left_scope,
            right_scope,
        }
    }

    pub fn extract(&self, exprs: Vec<Expression>) -> Result<ExtractedConditions> {
        // Split on AND first.
        let mut split_exprs = Vec::with_capacity(exprs.len());
        for expr in exprs {
            split_conjunction(expr, &mut split_exprs);
        }

        unimplemented!()
    }

    /// Finds the side of a join an expression is referencing.
    fn join_side(&self, expr: &Expression) -> Result<ExprJoinSide> {
        self.join_side_inner(expr, ExprJoinSide::None)
    }

    fn join_side_inner(&self, expr: &Expression, side: ExprJoinSide) -> Result<ExprJoinSide> {
        match expr {
            Expression::Column(col) => {
                if self
                    .bind_context
                    .table_is_in_scope(self.left_scope, col.table_scope)?
                {
                    Ok(ExprJoinSide::Left)
                } else if self
                    .bind_context
                    .table_is_in_scope(self.right_scope, col.table_scope)?
                {
                    Ok(ExprJoinSide::Left)
                } else {
                    Err(RayexecError::new(format!(
                        "Cannot find join side for expression: {expr}"
                    )))
                }
            }
            Expression::Subquery(_) => not_implemented!("subquery in join condition"),
            other => {
                let mut side = side;
                other.for_each_child(&mut |expr| {
                    let new_side = self.join_side_inner(expr, side)?;
                    side = new_side.combine(side);
                    Ok(())
                })?;
                Ok(side)
            }
        }
    }
}

/// Recursively split an expression on AND, putting the split expressions in
/// `out`.
fn split_conjunction(expr: Expression, out: &mut Vec<Expression>) {
    fn inner(expr: Expression, out: &mut Vec<Expression>) -> Option<Expression> {
        if let Expression::Conjunction(ConjunctionExpr {
            left,
            right,
            op: ConjunctionOperator::And,
        }) = expr
        {
            out.push(*left);
            if let Some(other_expr) = inner(*right, out) {
                out.push(other_expr);
            }
            return None;
        }
        Some(expr)
    }

    let last = inner(expr, out).expect("tail expression");
    out.push(last)
}
