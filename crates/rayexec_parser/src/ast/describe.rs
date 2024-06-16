use crate::meta::{AstMeta, Raw};
use crate::{keywords::Keyword, parser::Parser};
use rayexec_error::Result;

use super::{AstParseable, ObjectReference, QueryNode};

// TODO: `DESCRIBE <file>` could be interesting.
#[derive(Debug, Clone, PartialEq)]
pub enum Describe<T: AstMeta> {
    Query(QueryNode<T>),
    Table(T::TableReference),
}

impl AstParseable for Describe<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::DESCRIBE)?;

        if QueryNode::is_query_node_start(parser) {
            let query = QueryNode::parse(parser)?;
            Ok(Describe::Query(query))
        } else {
            let table = ObjectReference::parse(parser)?;
            Ok(Describe::Table(table))
        }
    }
}
