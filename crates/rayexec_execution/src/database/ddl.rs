use rayexec_error::Result;
use std::fmt::Debug;
use std::task::{Context, Poll};

use super::create::{CreateScalarFunctionInfo, CreateSchemaInfo, CreateTableInfo};
use super::table::DataTable;

/// Primary interface for making modifications to a catalog.
pub trait CatalogModifier: Debug + Sync + Send {
    fn create_schema(&self, create: CreateSchemaInfo) -> Result<Box<dyn CreateFut<Output = ()>>>;
    fn drop_schema(&self, name: &str) -> Result<Box<dyn DropFut>>;

    fn create_table(
        &self,
        schema: &str,
        info: CreateTableInfo,
    ) -> Result<Box<dyn CreateFut<Output = Box<dyn DataTable>>>>; // TODO: The output might need to include some additional info like if the table was actually created or it already existed.
    fn drop_table(&self, schema: &str, name: &str) -> Result<Box<dyn DropFut>>; // TODO: Info

    fn create_scalar_function(
        &self,
        info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut<Output = ()>>>;
    fn create_aggregate_function(
        &self,
        info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut<Output = ()>>>;
}

pub trait CreateFut: Debug + Sync + Send {
    type Output;
    fn poll_create(&mut self, cx: &mut Context) -> Poll<Result<Self::Output>>;
}

pub trait DropFut: Debug + Sync + Send {
    fn poll_drop(&mut self, cx: &mut Context) -> Poll<Result<()>>;
}
