use std::task::Context;

use bytes::Bytes;
use futures::stream::BoxStream;
use rayexec_error::Result;
use rayexec_execution::{
    database::table::{DataTable, DataTableScan},
    execution::operators::PollPull,
};
use rayexec_io::AsyncReader;

use crate::reader::{CsvSchema, DialectOptions};

#[derive(Debug)]
pub struct SingleFileCsvDataTable {
    options: DialectOptions,
    csv_schema: CsvSchema,
    reader: Box<dyn AsyncReader>,
}

impl DataTable for SingleFileCsvDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct CsvFileScan {}

impl DataTableScan for CsvFileScan {
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull> {
        unimplemented!()
    }
}
