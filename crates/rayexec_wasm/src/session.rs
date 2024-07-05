use std::{rc::Rc, sync::Arc};

use crate::{errors::Result, runtime::WasmExecutionRuntime};
use rayexec_bullet::format::{FormatOptions, Formatter};
use rayexec_error::RayexecError;
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_parquet::ParquetDataSource;
use rayexec_shell::session::{ResultTable, SingleUserEngine};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmSession(pub(crate) SingleUserEngine);

#[wasm_bindgen]
impl WasmSession {
    pub fn try_new() -> Result<WasmSession> {
        let runtime = Arc::new(WasmExecutionRuntime::try_new()?);
        let registry = DataSourceRegistry::default()
            .with_datasource("memory", Box::new(MemoryDataSource))?
            .with_datasource("parquet", Box::new(ParquetDataSource))?;

        let engine = SingleUserEngine::new_with_runtime(runtime, registry)?;

        Ok(WasmSession(engine))
    }

    pub async fn query(&self, sql: &str) -> Result<WasmResultTables> {
        let tables = self.0.query(sql).await?.into_iter().map(Rc::new).collect();
        Ok(WasmResultTables(tables))
    }
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmResultTables(pub(crate) Vec<Rc<ResultTable>>);

#[wasm_bindgen]
impl WasmResultTables {
    pub fn get_tables(&self) -> Vec<WasmResultTable> {
        self.0
            .iter()
            .map(|table| {
                let mut first_row_indices = Vec::with_capacity(table.batches.len());
                let mut curr_idx = 0;
                for batch in &table.batches {
                    first_row_indices.push(curr_idx);
                    curr_idx += batch.num_rows();
                }

                WasmResultTable {
                    table: table.clone(),
                    first_row_indices,
                }
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmResultTable {
    pub(crate) table: Rc<ResultTable>,
    pub(crate) first_row_indices: Vec<usize>,
}

#[wasm_bindgen]
impl WasmResultTable {
    pub fn column_names(&self) -> Vec<String> {
        self.table
            .schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub fn num_rows(&self) -> usize {
        self.table.batches.iter().map(|b| b.num_rows()).sum()
    }

    pub fn format_cell(&self, col: usize, row: usize) -> Result<String> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions::new());

        let batch_idx = self
            .first_row_indices
            .iter()
            .position(|&idx| row >= idx)
            .unwrap();

        let arr = self.table.batches[batch_idx]
            .column(col)
            .ok_or_else(|| RayexecError::new(format!("Column index {col} out of range")))?;

        let v = FORMATTER
            .format_array_value(arr, row - self.first_row_indices[batch_idx])
            .ok_or_else(|| RayexecError::new(format!("Row index {row} out of range")))?;

        Ok(v.to_string())
    }
}
