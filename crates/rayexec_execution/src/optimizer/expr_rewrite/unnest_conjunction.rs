use crate::expr::{
    conjunction_expr::{ConjunctionExpr, ConjunctionOperator},
    Expression,
};
use rayexec_error::Result;

use super::ExpressionRewriteRule;

/// Unnest nested AND or OR expressions.
///
/// 'a AND (b AND c) => a AND b AND c'
#[derive(Debug)]
pub struct UnnestConjunctionRewrite;

impl ExpressionRewriteRule for UnnestConjunctionRewrite {
    fn rewrite(mut expression: Expression) -> Result<Expression> {
        fn inner(expression: &mut Expression) {
            match expression {
                Expression::Conjunction(ConjunctionExpr { op, expressions }) => {
                    let mut new_expressions = Vec::with_capacity(expressions.len());
                    for expr in expressions.drain(..) {
                        unnest_op(expr, *op, &mut new_expressions);
                    }

                    *expression = Expression::Conjunction(ConjunctionExpr {
                        op: *op,
                        expressions: new_expressions,
                    })
                }
                other => other
                    .for_each_child_mut(&mut |child| {
                        inner(child);
                        Ok(())
                    })
                    .expect("unnest to not fail"),
            }
        }

        inner(&mut expression);

        Ok(expression)
    }
}

fn unnest_op(expr: Expression, search_op: ConjunctionOperator, out: &mut Vec<Expression>) {
    match expr {
        Expression::Conjunction(ConjunctionExpr { op, expressions }) if op == search_op => {
            for expr in expressions {
                unnest_op(expr, search_op, out);
            }
        }
        other => out.push(other),
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::ScalarValue;

    use crate::expr::literal_expr::LiteralExpr;

    use super::*;

    #[test]
    fn unnest_none() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(0),
                }),
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(1),
                }),
            ],
        });

        // No change.
        let expected = expr.clone();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn unnest_one_level() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(0),
                }),
                Expression::Conjunction(ConjunctionExpr {
                    op: ConjunctionOperator::And,
                    expressions: vec![
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Int8(1),
                        }),
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Int8(2),
                        }),
                    ],
                }),
            ],
        });

        let expected = Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(0),
                }),
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(1),
                }),
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(2),
                }),
            ],
        });

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn no_unnest_different_ops() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(0),
                }),
                Expression::Conjunction(ConjunctionExpr {
                    op: ConjunctionOperator::Or,
                    expressions: vec![
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Int8(1),
                        }),
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Int8(2),
                        }),
                    ],
                }),
            ],
        });

        // No change.
        let expected = expr.clone();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn no_unnest_different_ops_nested() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(0),
                }),
                Expression::Conjunction(ConjunctionExpr {
                    op: ConjunctionOperator::Or,
                    expressions: vec![
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Int8(1),
                        }),
                        Expression::Conjunction(ConjunctionExpr {
                            op: ConjunctionOperator::And,
                            expressions: vec![
                                Expression::Literal(LiteralExpr {
                                    literal: ScalarValue::Int8(2),
                                }),
                                Expression::Literal(LiteralExpr {
                                    literal: ScalarValue::Int8(3),
                                }),
                            ],
                        }),
                    ],
                }),
            ],
        });

        // No change.
        let expected = expr.clone();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn unnest_three_levels() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(0),
                }),
                Expression::Conjunction(ConjunctionExpr {
                    op: ConjunctionOperator::And,
                    expressions: vec![
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Int8(1),
                        }),
                        Expression::Conjunction(ConjunctionExpr {
                            op: ConjunctionOperator::And,
                            expressions: vec![
                                Expression::Literal(LiteralExpr {
                                    literal: ScalarValue::Int8(2),
                                }),
                                Expression::Literal(LiteralExpr {
                                    literal: ScalarValue::Int8(3),
                                }),
                            ],
                        }),
                    ],
                }),
            ],
        });

        let expected = Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(0),
                }),
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(1),
                }),
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(2),
                }),
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Int8(3),
                }),
            ],
        });

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }
}
