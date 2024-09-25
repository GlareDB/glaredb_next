mod errors;
mod event_loop;
mod session;

use std::sync::OnceLock;

use pyo3::{
    pyclass, pymodule,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyResult,
};

/// Defines the root python module.
///
/// 'name' needs to be the same name as the 'lib.name' field in the Cargo.toml.
#[pymodule(name = "rayexec")]
fn binding_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(session::connect, m)?)
}
