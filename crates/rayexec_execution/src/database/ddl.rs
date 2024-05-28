use rayexec_error::Result;
use std::fmt::Debug;
use std::task::{Context, Poll};

use super::create::{CreateScalarFunctionInfo, CreateTableInfo};

pub trait SchemaModifier: Debug + Sync + Send {
    fn create_table(&self, info: CreateTableInfo) -> Result<Box<dyn CreateFut>>;
    fn drop_table(&self, name: &str) -> Result<Box<dyn DropFut>>; // TODO: Info

    fn create_scalar_function(&self, info: CreateScalarFunctionInfo) -> Result<Box<dyn CreateFut>>;
    fn create_aggregate_function(
        &self,
        info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut>>;
}

pub trait CreateFut: Debug + Sync + Send {
    fn poll_create(&mut self, cx: &mut Context) -> Poll<Result<()>>;
}

pub trait DropFut: Debug + Sync + Send {
    fn poll_drop(&mut self, cx: &mut Context) -> Poll<Result<()>>;
}
