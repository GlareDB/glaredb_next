use futures::future::BoxFuture;
use rayexec_error::Result;
use rayexec_execution::{
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::ExecutionRuntime,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadDelta;

impl TableFunction for ReadDelta {
    fn name(&self) -> &'static str {
        "read_delta"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["delta_scan"]
    }

    fn plan_and_initialize<'a>(
        &'a self,
        runtime: &'a Arc<dyn ExecutionRuntime>,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        unimplemented!()
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadDeltaImpl {}
