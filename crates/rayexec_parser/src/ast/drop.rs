use crate::{keywords::Keyword, parser::Parser};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, ObjectReference};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropType {
    Index,
    Function,
    Table,
    View,
    Schema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropDependents {
    #[default]
    Restrict,
    Cascade,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropStatement {
    pub drop_type: DropType,
    pub if_exists: bool,
    pub name: ObjectReference,
    pub deps: DropDependents,
}

impl AstParseable for DropStatement {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::DROP)?;

        let drop_type = match parser.next_keyword()? {
            Keyword::TABLE => DropType::Table,
            Keyword::INDEX => DropType::Index,
            Keyword::FUNCTION => DropType::Function,
            Keyword::SCHEMA => DropType::Schema,
            Keyword::VIEW => DropType::View,
            other => {
                return Err(RayexecError::new(format!(
                    "Got unexpected keyword for drop type: {other}"
                )))
            }
        };

        let if_exists = parser.parse_keyword_sequence(&[Keyword::IF, Keyword::EXISTS]);
        let name = ObjectReference::parse(parser)?;

        let deps = if parser.parse_keyword(Keyword::CASCADE) {
            DropDependents::Cascade
        } else if parser.parse_keyword(Keyword::RESTRICT) {
            DropDependents::Restrict
        } else {
            DropDependents::Restrict
        };

        Ok(DropStatement {
            drop_type,
            if_exists,
            name,
            deps,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::testutil::parse_ast;

    use super::*;

    #[test]
    fn basic() {
        let got = parse_ast::<DropStatement>("drop schema my_schema").unwrap();
        let expected = DropStatement {
            drop_type: DropType::Schema,
            if_exists: false,
            name: ObjectReference::from_strings(["my_schema"]),
            deps: DropDependents::Restrict,
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn drop_table_cascade() {
        let got = parse_ast::<DropStatement>("drop table my_schema.t1 cascade").unwrap();
        let expected = DropStatement {
            drop_type: DropType::Table,
            if_exists: false,
            name: ObjectReference::from_strings(["my_schema", "t1"]),
            deps: DropDependents::Cascade,
        };
        assert_eq!(expected, got);
    }
}
