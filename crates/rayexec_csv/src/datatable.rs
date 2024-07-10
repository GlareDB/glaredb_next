use std::task::Context;

use rayexec_error::Result;
use rayexec_execution::{
    database::table::{DataTable, DataTableScan},
    execution::operators::PollPull,
};

#[derive(Debug)]
pub struct MultiFileCsvDataTable {}

impl DataTable for MultiFileCsvDataTable {
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
