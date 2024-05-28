use crate::{keywords::Keyword, parser::Parser};
use rayexec_error::Result;

use super::{AstParseable, DataType, Ident, ObjectReference};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub temp: bool,
    pub external: bool,
    pub name: ObjectReference,
    pub columns: Vec<ColumnDef>,
}

impl AstParseable for CreateTable {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::CREATE)?;

        let or_replace = parser.parse_keyword_sequence(&[Keyword::OR, Keyword::REPLACE]);
        let temp = parser
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();
        let external = parser.parse_keyword(Keyword::EXTERNAL);

        parser.expect_keyword(Keyword::TABLE)?;

        let if_not_exists =
            parser.parse_keyword_sequence(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = ObjectReference::parse(parser)?;
        let columns = parser.parse_parenthesized_comma_separated(ColumnDef::parse)?;

        Ok(CreateTable {
            or_replace,
            if_not_exists,
            temp,
            external,
            name,
            columns,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: Ident,
    pub datatype: DataType,
    pub opts: Vec<ColumnOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnOption {
    Null,
    NotNull,
}

impl AstParseable for ColumnDef {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let name = Ident::parse(parser)?;
        let datatype = DataType::parse(parser)?;

        let mut opts = Vec::new();

        if parser.parse_keyword_sequence(&[Keyword::NOT, Keyword::NULL]) {
            opts.push(ColumnOption::NotNull)
        }
        if parser.parse_keyword(Keyword::NULL) {
            opts.push(ColumnOption::Null)
        }

        Ok(ColumnDef {
            name,
            datatype,
            opts,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::testutil::parse_ast;

    use super::*;

    #[test]
    fn basic() {
        let got = parse_ast::<CreateTable>("create table hello (a int)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: false,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![ColumnDef {
                name: Ident::from_string("a"),
                datatype: DataType::Integer,
                opts: Vec::new(),
            }],
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn two_columns() {
        let got = parse_ast::<CreateTable>("create table hello (a int, world text)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: false,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![
                ColumnDef {
                    name: Ident::from_string("a"),
                    datatype: DataType::Integer,
                    opts: Vec::new(),
                },
                ColumnDef {
                    name: Ident::from_string("world"),
                    datatype: DataType::Varchar(None),
                    opts: Vec::new(),
                },
            ],
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn two_columns_trailing_comma() {
        let got = parse_ast::<CreateTable>("create table hello (a int, world text,)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: false,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![
                ColumnDef {
                    name: Ident::from_string("a"),
                    datatype: DataType::Integer,
                    opts: Vec::new(),
                },
                ColumnDef {
                    name: Ident::from_string("world"),
                    datatype: DataType::Varchar(None),
                    opts: Vec::new(),
                },
            ],
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn temp() {
        let got = parse_ast::<CreateTable>("create temp table hello (a int)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: true,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![ColumnDef {
                name: Ident::from_string("a"),
                datatype: DataType::Integer,
                opts: Vec::new(),
            }],
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn temp_if_not_exists() {
        let got =
            parse_ast::<CreateTable>("create temp table if not exists hello (a int)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: true,
            temp: true,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![ColumnDef {
                name: Ident::from_string("a"),
                datatype: DataType::Integer,
                opts: Vec::new(),
            }],
        };
        assert_eq!(expected, got);
    }
}
