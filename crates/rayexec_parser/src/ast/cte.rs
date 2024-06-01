use crate::{keywords::Keyword, parser::Parser, tokens::Token};
use rayexec_error::Result;

use super::{AstParseable, Ident, QueryNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CteDefs {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
}

impl AstParseable for CteDefs {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let recursive = parser.parse_keyword(Keyword::RECURSIVE);
        Ok(CteDefs {
            recursive,
            ctes: parser.parse_comma_separated(Cte::parse)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cte {
    pub alias: Ident,
    pub column_aliases: Option<Vec<Ident>>,
    pub materialized: bool,
    pub body: Box<QueryNode>,
}

impl AstParseable for Cte {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let alias = Ident::parse(parser)?;

        let column_aliases = if parser.parse_keyword(Keyword::AS) {
            // No aliases specified.
            //
            // `alias AS (<subquery>)`
            None
        } else {
            // Aliases specified.
            //
            // `alias(c1, c2) AS (<subquery>)`
            parser.expect_token(&Token::LeftParen)?;
            let column_aliases = parser.parse_parenthesized_comma_separated(Ident::parse)?;
            parser.expect_token(&Token::RightParen)?;
            parser.expect_keyword(Keyword::AS)?;
            Some(column_aliases)
        };

        let materialized = parser.parse_keyword(Keyword::MATERIALIZED);

        // Parse the subquery.
        parser.expect_token(&Token::LeftParen)?;
        let body = QueryNode::parse(parser)?;
        parser.expect_token(&Token::RightParen)?;

        Ok(Cte {
            alias,
            column_aliases,
            materialized,
            body: Box::new(body),
        })
    }
}
