use crate::expr::{
    conjunction_expr::{ConjunctionExpr, ConjunctionOperator},
    Expression,
};

/// Recursively split an expression on AND, putting the split expressions in
/// `out`.
pub fn split_conjunction(expr: Expression, out: &mut Vec<Expression>) {
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

    if let Some(expr) = inner(expr, out) {
        out.push(expr)
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::ScalarValue;

    use crate::expr::literal_expr::LiteralExpr;

    use super::*;

    #[test]
    fn split_conjunction_none() {
        let expr = Expression::Literal(LiteralExpr {
            literal: ScalarValue::Int8(4),
        });

        let mut out = Vec::new();
        split_conjunction(expr.clone(), &mut out);

        let expected = vec![expr];
        assert_eq!(expected, out);
    }

    #[test]
    fn split_conjunction_single_and() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            left: Box::new(Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            })),
            right: Box::new(Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(false),
            })),
            op: ConjunctionOperator::And,
        });

        let mut out = Vec::new();
        split_conjunction(expr, &mut out);

        let expected = vec![
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            }),
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(false),
            }),
        ];
        assert_eq!(expected, out);
    }

    #[test]
    fn split_conjunction_nested_and() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            left: Box::new(Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            })),
            right: Box::new(Expression::Conjunction(ConjunctionExpr {
                left: Box::new(Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(true),
                })),
                right: Box::new(Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(false),
                })),
                op: ConjunctionOperator::And,
            })),
            op: ConjunctionOperator::And,
        });

        let mut out = Vec::new();
        split_conjunction(expr, &mut out);

        let expected = vec![
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            }),
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            }),
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(false),
            }),
        ];
        assert_eq!(expected, out);
    }
}
