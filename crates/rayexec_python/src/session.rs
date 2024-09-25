use pyo3::{pyclass, pyfunction};

use crate::errors::Result;

use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_execution::datasource::{DataSourceBuilder, DataSourceRegistry, MemoryDataSource};
use rayexec_parquet::ParquetDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;

#[pyfunction]
pub fn connect() -> Result<PythonSession> {
    // TODO: Pass in a tokio runtime.
    let runtime = NativeRuntime::with_default_tokio()?;
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("parquet", ParquetDataSource::initialize(runtime.clone()))?
        .with_datasource("csv", CsvDataSource::initialize(runtime.clone()))?
        .with_datasource("delta", DeltaDataSource::initialize(runtime.clone()))?;

    let executor = ThreadedNativeExecutor::try_new()?;
    let engine = SingleUserEngine::try_new(executor, runtime.clone(), registry)?;

    Ok(PythonSession { runtime, engine })
}

#[pyclass]
#[derive(Debug)]
pub struct PythonSession {
    pub(crate) runtime: NativeRuntime,
    pub(crate) engine: SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>,
}
