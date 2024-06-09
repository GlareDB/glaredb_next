use std::path::PathBuf;

use rayexec_error::{RayexecError, Result};
use rayexec_execution::functions::table::{
    check_named_args_is_empty, GenericTableFunction, SpecializedTableFunction, TableFunctionArgs,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadParquet;

impl GenericTableFunction for ReadParquet {
    fn name(&self) -> &'static str {
        "read_parquet"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["parquet_scan"]
    }

    fn specialize(&self, args: &TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        check_named_args_is_empty(self, args)?;

        // Will be more once the http/object storage stuff is added.
        if args.positional.len() != 1 {
            return Err(RayexecError::new(format!(
                "'{}' expected one argument",
                self.name()
            )));
        }

        // TODO: Globbing

        let path = PathBuf::from(args.positional[0].try_as_str()?);

        unimplemented!()
    }
}
