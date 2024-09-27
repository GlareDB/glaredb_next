use pyo3::{pyclass, pymethods};
use rayexec_shell::session::ResultTable2;

use crate::errors::Result;

// TODO: Lazy
#[pyclass]
#[derive(Debug)]
pub struct PythonTable {
    pub(crate) table: ResultTable2,
}

#[pymethods]
impl PythonTable {
    fn __repr__(&mut self) -> Result<String> {
        let pretty = self.table.pretty_table(80, None)?;
        Ok(format!("{pretty}"))
    }
}
