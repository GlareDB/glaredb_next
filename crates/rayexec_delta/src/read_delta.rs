use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{
        check_named_args_is_empty, PlannedTableFunction, TableFunction, TableFunctionArgs,
    },
    runtime::ExecutionRuntime,
};
use rayexec_io::FileLocation;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::protocol::table::Table;

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
        Box::pin(async move { ReadDeltaImpl::initialize(runtime.as_ref(), args).await })
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(ReadDeltaImpl::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadDeltaImpl {
    location: FileLocation,
    #[serde(skip)]
    table: Option<Arc<Table>>, // Populate on re-init if needed.
}

impl ReadDeltaImpl {
    async fn initialize(
        runtime: &dyn ExecutionRuntime,
        mut args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        check_named_args_is_empty(&ReadDelta, &args)?;
        if args.positional.len() != 1 {
            return Err(RayexecError::new("Expected one argument"));
        }

        let location = args.positional.pop().unwrap().try_into_string()?;
        let location = FileLocation::parse(&location);

        let provider = runtime.file_provider();

        let table = Table::load(location.clone(), provider).await?;

        Ok(Box::new(ReadDeltaImpl {
            location,
            table: Some(Arc::new(table)),
        }))
    }
}

impl PlannedTableFunction for ReadDeltaImpl {
    fn reinitialize(&self, _runtime: &Arc<dyn ExecutionRuntime>) -> BoxFuture<Result<()>> {
        // TODO: Reinit table.
        unimplemented!()
    }

    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn table_function(&self) -> &dyn TableFunction {
        &ReadDelta
    }

    fn schema(&self) -> Schema {
        unimplemented!()
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }
}
