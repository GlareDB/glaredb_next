use futures::{future::BoxFuture, StreamExt};
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{
        check_named_args_is_empty, GenericTableFunction, InitializedTableFunction,
        SpecializedTableFunction, TableFunctionArgs,
    },
    runtime::ExecutionRuntime,
};
use rayexec_io::{FileLocation, FileSource};
use std::sync::Arc;

use crate::{
    datatable::SingleFileCsvDataTable,
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

        let location = args.positional.pop().unwrap().try_into_string()?;
        let location = FileLocation::parse(&location);

        Ok(Box::new(ReadCsvImpl { location }))
    }
}

#[derive(Debug, Clone)]
struct ReadCsvImpl {
    location: FileLocation,
}

impl SpecializedTableFunction for ReadCsvImpl {
    fn name(&self) -> &'static str {
        "read_csv_local"
    }

    fn initialize(
        self: Box<Self>,
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        Box::pin(async move { self.initialize_inner(runtime.as_ref()).await })
    }
}

impl ReadCsvImpl {
    async fn initialize_inner(
        self,
        runtime: &dyn ExecutionRuntime,
    ) -> Result<Box<dyn InitializedTableFunction>> {
        let mut source = runtime.file_provider().file_source(self.location.clone())?;

        let mut stream = source.read_stream();
        // TODO: Actually make sure this is a sufficient size to infer from.
        // TODO: This throws away the buffer after inferring.
        let infer_buf = match stream.next().await {
            Some(result) => {
                const INFER_SIZE: usize = 1024;
                let buf = result?;
                if buf.len() > INFER_SIZE {
                    buf.slice(0..INFER_SIZE)
                } else {
                    buf
                }
            }
            None => return Err(RayexecError::new("Stream returned no data")),
        };

        let options = DialectOptions::infer_from_sample(&infer_buf)?;
        let mut decoder = CsvDecoder::new(options);
        let mut state = DecoderState::default();
        let _ = decoder.decode(&infer_buf, &mut state)?;
        let completed = state.completed_records();
        let schema = CsvSchema::infer_from_records(completed)?;

        Ok(Box::new(InitializedLocalCsvFunction {
            specialized: self,
            options,
            csv_schema: schema,
        }))
    }
}

#[derive(Debug, Clone)]
struct InitializedLocalCsvFunction {
    specialized: ReadCsvImpl,
    options: DialectOptions,
    csv_schema: CsvSchema,
}

impl InitializedTableFunction for InitializedLocalCsvFunction {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.specialized
    }

    fn schema(&self) -> Schema {
        Schema {
            fields: self.csv_schema.fields.clone(),
        }
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(SingleFileCsvDataTable {
            options: self.options,
            csv_schema: self.csv_schema.clone(),
            location: self.specialized.location.clone(),
            runtime: runtime.clone(),
        }))
    }
}
