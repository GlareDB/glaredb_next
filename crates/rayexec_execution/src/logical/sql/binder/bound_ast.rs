//! Protobuf conversions for bound variant of the ast.
use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;
use rayexec_parser::{ast, meta::AstMeta, statement::Statement};

use crate::{
    database::DatabaseContext,
    expr::scalar::{BinaryOperator, UnaryOperator},
    functions::table::TableFunctionArgs,
    proto::DatabaseProtoConv,
};

use super::{
    bind_data::{BindListIdx, CteReference, ItemReference},
    BoundCopyTo,
};

/// An AST statement with references bound to data inside of the `bind_data`.
pub type BoundStatement = Statement<Bound>;

/// Implementation of `AstMeta` which annotates the AST query with
/// tables/functions/etc found in the db.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound;

impl AstMeta for Bound {
    type DataSourceName = String;
    type ItemReference = ItemReference;
    type TableReference = BindListIdx;
    type TableFunctionReference = BindListIdx;
    // TODO: Having this be the actual table function args does require that we
    // clone them, and the args that go back into the ast don't actually do
    // anything, they're never referenced again.
    type TableFunctionArgs = TableFunctionArgs;
    type CteReference = CteReference;
    type FunctionReference = BindListIdx;
    type ColumnReference = String;
    type DataType = DataType;
    type CopyToDestination = BoundCopyTo; // TODO: Move this here.
    type BinaryOperator = BinaryOperator;
    type UnaryOperator = UnaryOperator;
}

impl DatabaseProtoConv for Statement<Bound> {
    type ProtoType = rayexec_proto::generated::ast::bound::Statement;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
    }
}
