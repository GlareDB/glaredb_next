use futures::{future::BoxFuture, StreamExt};
use rayexec_bullet::field::Schema;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{
        check_named_args_is_empty, GenericTableFunction, InitializedTableFunction,
        SpecializedTableFunction, TableFunctionArgs,
    },
    runtime::ExecutionRuntime,
};
use std::{path::PathBuf, sync::Arc};
use url::Url;

use crate::{
    decoder::{CsvDecoder, DecoderState},
    reader::{CsvSchema, DialectOptions},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCsv;

impl GenericTableFunction for ReadCsv {
    fn name(&self) -> &'static str {
        "read_csv"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["csv_scan"]
    }

    fn specialize(&self, mut args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        check_named_args_is_empty(self, &args)?;
        if args.positional.len() != 1 {
            return Err(RayexecError::new("Expected one argument"));
        }

        // TODO: Glob, dispatch to object storage/http impls

        let path = args.positional.pop().unwrap().try_into_string()?;

        match Url::parse(&path) {
            Ok(_) => not_implemented!("remote csv"),
            Err(_) => {
                // Assume file.
                unimplemented!()
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ReadCsvLocal {
    path: PathBuf,
}

impl SpecializedTableFunction for ReadCsvLocal {
    fn name(&self) -> &'static str {
        "read_csv_local"
    }

    fn initialize(
        self: Box<Self>,
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        unimplemented!()
    }
}

impl ReadCsvLocal {
    async fn initialize_inner(
        self,
        runtime: &dyn ExecutionRuntime,
    ) -> Result<Box<dyn InitializedTableFunction>> {
        let fs = runtime.filesystem()?;
        let mut file = fs.reader(&self.path)?;

        let mut stream = file.read_stream();
        // TODO: Actually make sure this is a sufficient size to infer from.
        let infer_buf = match stream.next().await {
            Some(result) => result?,
            None => return Err(RayexecError::new("Stream returned no data")),
        };

        let options = DialectOptions::infer_from_sample(&infer_buf)?;
        let mut decoder = CsvDecoder::new(options);
        let mut state = DecoderState::default();
        let _ = decoder.decode(&infer_buf, &mut state)?;
        let completed = state.completed_records();
        let schema = CsvSchema::infer_from_records(completed)?;

        unimplemented!()
    }
}

#[derive(Debug, Clone)]
struct InitializedLocalCsvFunction {
    specialized: ReadCsvLocal,
    options: DialectOptions,
    csv_schema: CsvSchema,
}

impl InitializedTableFunction for InitializedLocalCsvFunction {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.specialized
    }

    fn schema(&self) -> Schema {
        unimplemented!()
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }
}
