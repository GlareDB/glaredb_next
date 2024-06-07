use rayexec_error::{RayexecError, Result};

use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
    tokens::{Token, Word},
};

use super::{AstParseable, Ident, ObjectReference, QueryNode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    /// Plus, e.g. `+9`
    Plus,
    /// Minus, e.g. `-9`
    Minus,
    /// Not, e.g. `NOT(true)`
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    /// Plus, e.g. `a + b`
    Plus,
    /// Minus, e.g. `a - b`
    Minus,
    /// Multiply, e.g. `a * b`
    Multiply,
    /// Divide, e.g. `a / b`
    Divide,
    /// Integer division, e.g. `a // b`
    IntDiv,
    /// Modulo, e.g. `a % b`
    Modulo,
    /// String/Array Concat operator, e.g. `a || b`
    StringConcat,
    /// Greater than, e.g. `a > b`
    Gt,
    /// Less than, e.g. `a < b`
    Lt,
    /// Greater equal, e.g. `a >= b`
    GtEq,
    /// Less equal, e.g. `a <= b`
    LtEq,
    /// Spaceship, e.g. `a <=> b`
    Spaceship,
    /// Equal, e.g. `a = b`
    Eq,
    /// Not equal, e.g. `a <> b`
    NotEq,
    /// And, e.g. `a AND b`
    And,
    /// Or, e.g. `a OR b`
    Or,
    /// XOR, e.g. `a XOR b`
    Xor,
    /// Bitwise or, e.g. `a | b`
    BitwiseOr,
    /// Bitwise and, e.g. `a & b`
    BitwiseAnd,
    /// Bitwise XOR, e.g. `a ^ b`
    BitwiseXor,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal<T: AstMeta> {
    /// Unparsed number literal.
    Number(String),
    /// String literal.
    SingleQuotedString(String),
    /// Boolean literal.
    Boolean(bool),
    /// Null literal
    Null,
    /// Struct literal.
    ///
    /// Lengths of keys and values must be the same.
    Struct {
        keys: Vec<String>,
        values: Vec<Expr<T>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Function<T: AstMeta> {
    pub reference: T::FunctionReference,
    pub args: Vec<FunctionArg<T>>,
    /// Filter part of `COUNT(col) FILTER (WHERE col > 5)`
    pub filter: Option<Box<Expr<T>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg<T: AstMeta> {
    /// A named argument. Allows use of either `=>` or `=` for assignment.
    ///
    /// `ident => <expr>` or `ident = <expr>`
    Named {
        name: Ident,
        arg: FunctionArgExpr<T>,
    },
    /// `<expr>`
    Unnamed { arg: FunctionArgExpr<T> },
}

impl AstParseable for FunctionArg<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let is_named = match parser.peek_nth(1) {
            Some(tok) => matches!(tok.token, Token::RightArrow | Token::Eq),
            None => false,
        };

        if is_named {
            let ident = Ident::parse(parser)?;
            parser.expect_one_of_tokens(&[&Token::RightArrow, &Token::Eq])?;
            let expr = FunctionArgExpr::parse(parser)?;

            Ok(FunctionArg::Named {
                name: ident,
                arg: expr,
            })
        } else {
            let expr = FunctionArgExpr::parse(parser)?;
            Ok(FunctionArg::Unnamed { arg: expr })
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArgExpr<T: AstMeta> {
    Wildcard,
    Expr(Expr<T>),
}

impl AstParseable for FunctionArgExpr<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        match parser.peek() {
            Some(tok) if tok.token == Token::Mul => {
                let _ = parser.next(); // Consume.
                Ok(Self::Wildcard)
            }
            _ => Ok(Self::Expr(Expr::parse(parser)?)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr<T: AstMeta> {
    /// Column or table identifier.
    Ident(Ident),
    /// Compound identifier.
    ///
    /// `table.col`
    CompoundIdent(Vec<Ident>),
    /// Identifier followed by '*'.
    ///
    /// `table.*`
    QualifiedWildcard(Vec<Ident>),
    /// An expression literal,
    Literal(Literal<T>),
    /// Unary expression.
    UnaryExpr {
        op: UnaryOperator,
        expr: Box<Expr<T>>,
    },
    /// A binary expression.
    BinaryExpr {
        left: Box<Expr<T>>,
        op: BinaryOperator,
        right: Box<Expr<T>>,
    },
    /// A function call.
    Function(Function<T>),
    /// Scalar subquery.
    Subquery(Box<QueryNode<T>>),
    /// Nested expression wrapped in parenthesis.
    ///
    /// (1 + 2)
    Nested(Box<Expr<T>>),
    /// Tuple of expressions.
    ///
    /// (1, 2)
    Tuple(Vec<Expr<T>>),
    /// A colation.
    ///
    /// `<expr> COLLATE <collation>`
    Collate {
        expr: Box<Expr<T>>,
        collation: ObjectReference,
    },
    /// EXISTS/NOT EXISTS
    Exists {
        subquery: Box<QueryNode<T>>,
        not_exists: bool,
    },
}

impl AstParseable for Expr<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Self::parse_subexpr(parser, 0)
    }
}

impl Expr<Raw> {
    // Precdences, ordered low to high.
    const PREC_OR: u8 = 10;
    const PREC_AND: u8 = 20;
    const PREC_NOT: u8 = 30;
    const PREC_IS: u8 = 40;
    const PREC_COMPARISON: u8 = 50; // <=, =, etc
    const PREC_CONTAINMENT: u8 = 60; // BETWEEN, IN, LIKE, etc
    const PREC_EVERYTHING_ELSE: u8 = 70; // Anything without a specific precedence.
    const PREC_ADD_SUB: u8 = 80;
    const PREC_MUL_DIV_MOD: u8 = 90;
    const _PREC_EXPONENTIATION: u8 = 100;
    const _PREC_AT: u8 = 110; // AT TIME ZONE
    const _PREC_COLLATE: u8 = 120;
    const PREC_ARRAY_ELEM: u8 = 130; // []
    const PREC_CAST: u8 = 140; // ::

    fn parse_subexpr(parser: &mut Parser, precendence: u8) -> Result<Self> {
        let mut expr = Expr::parse_prefix(parser)?;

        loop {
            let next_precedence = Self::get_infix_precedence(parser)?;
            if precendence >= next_precedence {
                break;
            }

            expr = Self::parse_infix(parser, expr, next_precedence)?;
        }

        Ok(expr)
    }

    fn parse_prefix(parser: &mut Parser) -> Result<Self> {
        // TODO: Typed string

        let tok = match parser.next() {
            Some(tok) => tok,
            None => {
                return Err(RayexecError::new(
                    "Expected prefix expression, found end of statement",
                ))
            }
        };

        let expr = match &tok.token {
            Token::Word(w) => match w.keyword {
                Some(kw) => match kw {
                    Keyword::TRUE => Expr::Literal(Literal::Boolean(true)),
                    Keyword::FALSE => Expr::Literal(Literal::Boolean(false)),
                    Keyword::NULL => Expr::Literal(Literal::Null),
                    Keyword::EXISTS => {
                        parser.expect_token(&Token::LeftParen)?;
                        let subquery = QueryNode::parse(parser)?;
                        parser.expect_token(&Token::RightParen)?;
                        Expr::Exists {
                            subquery: Box::new(subquery),
                            not_exists: false,
                        }
                    }
                    Keyword::NOT => match parser.peek().map(|t| &t.token) {
                        Some(Token::Word(w)) if w.keyword == Some(Keyword::EXISTS) => {
                            parser.expect_keyword(Keyword::EXISTS)?;
                            parser.expect_token(&Token::LeftParen)?;
                            let subquery = QueryNode::parse(parser)?;
                            parser.expect_token(&Token::RightParen)?;
                            Expr::Exists {
                                subquery: Box::new(subquery),
                                not_exists: true,
                            }
                        }
                        _ => Expr::UnaryExpr {
                            op: UnaryOperator::Not,
                            expr: Box::new(Expr::parse_subexpr(parser, Self::PREC_NOT)?),
                        },
                    },
                    _ => Self::parse_ident_expr(w.clone(), parser)?,
                },
                None => Self::parse_ident_expr(w.clone(), parser)?,
            },
            Token::SingleQuotedString(s) => Expr::Literal(Literal::SingleQuotedString(s.clone())),
            Token::Number(s) => Expr::Literal(Literal::Number(s.clone())),
            Token::LeftParen => {
                let expr = if QueryNode::is_query_node_start(parser) {
                    let subquery = QueryNode::parse(parser)?;
                    Expr::Subquery(Box::new(subquery))
                } else {
                    let mut exprs = parser.parse_comma_separated(Expr::parse)?;
                    match exprs.len() {
                        0 => return Err(RayexecError::new("No expressions")),
                        1 => Expr::Nested(Box::new(exprs.pop().unwrap())),
                        _ => Expr::Tuple(exprs),
                    }
                };
                parser.expect_token(&Token::RightParen)?;
                expr
            }
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected token '{other:?}'. Expected expression."
                )))
            }
        };

        Ok(expr)
    }

    fn parse_infix(parser: &mut Parser, prefix: Expr<Raw>, precendence: u8) -> Result<Self> {
        let tok = match parser.next() {
            Some(tok) => &tok.token,
            None => {
                return Err(RayexecError::new(
                    "Expected infix expression, found end of statement",
                ))
            }
        };

        let bin_op: Option<BinaryOperator> = match tok {
            Token::DoubleEq => Some(BinaryOperator::Eq),
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mul => Some(BinaryOperator::Multiply),
            Token::Div => Some(BinaryOperator::Divide),
            Token::IntDiv => Some(BinaryOperator::IntDiv),
            Token::Mod => Some(BinaryOperator::Modulo),
            Token::Concat => Some(BinaryOperator::StringConcat),
            Token::Word(w) => match w.keyword {
                Some(Keyword::AND) => Some(BinaryOperator::And),
                Some(Keyword::OR) => Some(BinaryOperator::Or),
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = bin_op {
            if let Some(_kw) = parser.parse_one_of_keywords(&[Keyword::ALL, Keyword::ANY]) {
                unimplemented!()
            } else {
                Ok(Expr::BinaryExpr {
                    left: Box::new(prefix),
                    op,
                    right: Box::new(Expr::parse_subexpr(parser, precendence)?),
                })
            }
        } else if tok == &Token::LeftBracket {
            // Array index
            unimplemented!()
        } else if tok == &Token::DoubleColon {
            // Cast
            unimplemented!()
        } else {
            Err(RayexecError::new(format!(
                "Unable to parse token {:?} as an expression",
                tok
            )))
        }
    }

    /// Get the relative precedence of the next operator.
    ///
    /// If the operator is right associative, it's not considered an infix
    /// operator and zero will be returned.
    ///
    /// See <https://www.postgresql.org/docs/16/sql-syntax-lexical.html#SQL-PRECEDENCE>
    fn get_infix_precedence(parser: &mut Parser) -> Result<u8> {
        let tok = match parser.peek() {
            Some(tok) => &tok.token,
            None => return Ok(0),
        };

        match tok {
            Token::Word(w) if w.keyword == Some(Keyword::OR) => Ok(Self::PREC_OR),
            Token::Word(w) if w.keyword == Some(Keyword::AND) => Ok(Self::PREC_AND),

            Token::Word(w) if w.keyword == Some(Keyword::NOT) => {
                // Precedence depends on keyword following it.
                let next_kw = match parser.peek_nth(1) {
                    Some(tok) => match tok.keyword() {
                        Some(kw) => kw,
                        None => return Ok(0),
                    },
                    None => return Ok(0),
                };

                match next_kw {
                    Keyword::IN => Ok(Self::PREC_CONTAINMENT),
                    Keyword::BETWEEN => Ok(Self::PREC_CONTAINMENT),
                    Keyword::LIKE => Ok(Self::PREC_CONTAINMENT),
                    Keyword::ILIKE => Ok(Self::PREC_CONTAINMENT),
                    Keyword::RLIKE => Ok(Self::PREC_CONTAINMENT),
                    Keyword::REGEXP => Ok(Self::PREC_CONTAINMENT),
                    Keyword::SIMILAR => Ok(Self::PREC_CONTAINMENT),
                    _ => Ok(0),
                }
            }

            Token::Word(w) if w.keyword == Some(Keyword::IS) => {
                let next_kw = match parser.peek_nth(1) {
                    Some(tok) => match tok.keyword() {
                        Some(kw) => kw,
                        None => return Ok(0),
                    },
                    None => return Ok(0),
                };

                match next_kw {
                    Keyword::NULL => Ok(Self::PREC_IS),
                    _ => Ok(Self::PREC_IS),
                }
            }
            Token::Word(w) if w.keyword == Some(Keyword::IN) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::BETWEEN) => Ok(Self::PREC_CONTAINMENT),

            // "LIKE"
            Token::Word(w) if w.keyword == Some(Keyword::LIKE) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::ILIKE) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::RLIKE) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::REGEXP) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::SIMILAR) => Ok(Self::PREC_CONTAINMENT),

            // Equalities
            Token::Eq
            | Token::DoubleEq
            | Token::Neq
            | Token::Lt
            | Token::LtEq
            | Token::Gt
            | Token::GtEq => Ok(Self::PREC_COMPARISON),

            // Numeric operators
            Token::Plus | Token::Minus => Ok(Self::PREC_ADD_SUB),
            Token::Mul | Token::Div | Token::IntDiv | Token::Mod => Ok(Self::PREC_MUL_DIV_MOD),

            // Cast
            Token::DoubleColon => Ok(Self::PREC_CAST),

            // Concat
            Token::Concat => Ok(Self::PREC_EVERYTHING_ELSE),

            // Array, struct literals
            Token::LeftBrace | Token::LeftBracket => Ok(Self::PREC_ARRAY_ELEM),

            _ => Ok(0),
        }
    }

    /// Handle parsing expressions containing identifiers, starting with a word
    /// that is known to already be part of an identifier.
    fn parse_ident_expr(w: Word, parser: &mut Parser) -> Result<Expr<Raw>> {
        let mut wildcard = false;
        let mut idents = vec![Ident::from(w)];

        // Possibly compound identifier.
        while parser.consume_token(&Token::Period) {
            match parser.next() {
                Some(tok) => match &tok.token {
                    Token::Word(w) => idents.push(w.clone().into()),
                    Token::Mul => wildcard = true,
                    other => {
                        return Err(RayexecError::new(format!(
                            "Unexpected token in compound identifier: {other:?}"
                        )))
                    }
                },
                None => return Err(RayexecError::new("Expected identifier after '.'")),
            };
        }

        // Function call if left paren.
        if parser.consume_token(&Token::LeftParen) {
            if wildcard {
                // Someone trying to do this:
                // `namespace.*()`
                return Err(RayexecError::new("Cannot have wildcard function call"));
            }

            let args = parser.parse_comma_separated(FunctionArg::parse)?;
            parser.expect_token(&Token::RightParen)?;

            // FILTER (WHERE <expr>)
            let filter = if parser.parse_keyword(Keyword::FILTER) {
                parser.expect_token(&Token::LeftParen)?;
                parser.expect_keyword(Keyword::WHERE)?;
                let filter = Expr::parse(parser)?;
                parser.expect_token(&Token::RightParen)?;
                Some(Box::new(filter))
            } else {
                None
            };

            // TODO: Windows

            Ok(Expr::Function(Function {
                reference: ObjectReference(idents),
                args,
                filter,
            }))
        } else {
            Ok(match idents.len() {
                1 if !wildcard => Expr::Ident(idents.pop().unwrap()),
                _ => {
                    if wildcard {
                        Expr::QualifiedWildcard(idents)
                    } else {
                        Expr::CompoundIdent(idents)
                    }
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::testutil::parse_ast;

    use super::*;

    #[test]
    fn literal() {
        let expr: Expr<_> = parse_ast("5").unwrap();
        let expected = Expr::Literal(Literal::Number("5".to_string()));
        assert_eq!(expected, expr);
    }

    #[test]
    fn compound() {
        let expr: Expr<_> = parse_ast("my_schema.t1").unwrap();
        let expected = Expr::CompoundIdent(vec![
            Ident::from_string("my_schema"),
            Ident::from_string("t1"),
        ]);
        assert_eq!(expected, expr);
    }

    #[test]
    fn compound_with_keyword() {
        let expr: Expr<_> = parse_ast("schema.table").unwrap();
        let expected = Expr::CompoundIdent(vec![
            Ident::from_string("schema"),
            Ident::from_string("table"),
        ]);
        assert_eq!(expected, expr);
    }

    #[test]
    fn qualified_wildcard() {
        let expr: Expr<_> = parse_ast("schema.*").unwrap();
        let expected = Expr::QualifiedWildcard(vec![Ident::from_string("schema")]);
        assert_eq!(expected, expr);
    }

    #[test]
    fn binary_op() {
        let expr: Expr<_> = parse_ast("5 + 8").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::Literal(Literal::Number("5".to_string()))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Literal(Literal::Number("8".to_string()))),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn function_call_simple() {
        let expr: Expr<_> = parse_ast("sum(my_col)").unwrap();
        let expected = Expr::Function(Function {
            reference: ObjectReference(vec![Ident::from_string("sum")]),
            args: vec![FunctionArg::Unnamed {
                arg: FunctionArgExpr::Expr(Expr::Ident(Ident::from_string("my_col"))),
            }],
            filter: None,
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn function_call_with_over() {
        let expr: Expr<_> = parse_ast("count(x) filter (where x > 5)").unwrap();
        let expected = Expr::Function(Function {
            reference: ObjectReference(vec![Ident::from_string("count")]),
            args: vec![FunctionArg::Unnamed {
                arg: FunctionArgExpr::Expr(Expr::Ident(Ident::from_string("x"))),
            }],
            filter: Some(Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::Ident(Ident::from_string("x"))),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Literal::Number("5".to_string()))),
            })),
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn nested_expr() {
        let expr: Expr<_> = parse_ast("(1 + 2)").unwrap();
        let expected = Expr::Nested(Box::new(Expr::BinaryExpr {
            left: Box::new(Expr::Literal(Literal::Number("1".to_string()))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Literal(Literal::Number("2".to_string()))),
        }));
        assert_eq!(expected, expr);
    }

    #[test]
    fn count_star() {
        let expr: Expr<_> = parse_ast("count(*)").unwrap();
        let expected = Expr::Function(Function {
            reference: ObjectReference::from_strings(["count"]),
            args: vec![FunctionArg::Unnamed {
                arg: FunctionArgExpr::Wildcard,
            }],
            filter: None,
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn count_star_precedence_before() {
        let expr: Expr<_> = parse_ast("111 * count(*)").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::Literal(Literal::Number("111".to_string()))),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Function(Function {
                reference: ObjectReference::from_strings(["count"]),
                args: vec![FunctionArg::Unnamed {
                    arg: FunctionArgExpr::Wildcard,
                }],
                filter: None,
            })),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn count_star_precedence_after() {
        let expr: Expr<_> = parse_ast("count(*) * 111").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::Function(Function {
                reference: ObjectReference::from_strings(["count"]),
                args: vec![FunctionArg::Unnamed {
                    arg: FunctionArgExpr::Wildcard,
                }],
                filter: None,
            })),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Literal(Literal::Number("111".to_string()))),
        };
        assert_eq!(expected, expr);
    }
}
