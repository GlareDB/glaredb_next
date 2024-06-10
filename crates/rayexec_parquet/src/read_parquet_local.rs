use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::{
    database::table::DataTable, engine::EngineRuntime, functions::table::InitializedTableFunction,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReadParquetLocal {}
