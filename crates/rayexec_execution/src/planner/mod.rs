pub mod explainable;
pub mod expr;
pub mod operator;
pub mod plan;
pub mod scope;

use crate::{
    engine::vars::SessionVar,
    functions::{scalar::GenericScalarFunction, table::TableFunctionOld},
};
use rayexec_error::Result;
use rayexec_parser::ast;

pub trait Resolver: std::fmt::Debug {
    // fn resolve_aggregate_function(
    //     &self,
    //     reference: &ast::ObjectReference,
    // ) -> Result<Option<Box<dyn AggregateFunction>>>;

    fn resolve_scalar_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Option<Box<dyn GenericScalarFunction>>;

    fn resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunctionOld>>;

    fn get_session_variable(&self, name: &str) -> Result<SessionVar>;
}

#[derive(Debug)]
pub struct BindContext {}
