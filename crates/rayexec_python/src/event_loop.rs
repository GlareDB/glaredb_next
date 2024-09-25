use parking_lot::Mutex;
use pyo3::types::PyAnyMethods;
use rayexec_error::RayexecError;
use std::sync::{Arc, OnceLock};
use std::{cell::OnceCell, future::Future};

use pyo3::{Bound, IntoPy, PyAny, PyObject, Python};

use crate::errors::Result;

static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn tokio_handle() -> &'static tokio::runtime::Handle {
    let runtime = TOKIO_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .thread_name("rayexec_python_tokio")
            .build()
            .expect("to be able to build tokio runtime")
    });

    runtime.handle()
}

// asyncio docs: https://docs.python.org/3/library/asyncio.html

/// Runs a future until completion.
///
/// Executes the future on a global tokio runtime with the await happening in a
/// python asyncio event loop.
pub(crate) fn run_until_complete<F, T>(py: Python<'_>, fut: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    let event_loop = py.import_bound("asyncio")?.call_method0("new_event_loop")?;

    // TODO: Could be refcell.
    let output = Arc::new(Mutex::new(None));
    let py_future = spawn_python_future(py, event_loop.clone(), fut, output.clone())?;

    event_loop.call_method1("run_until_complete", (py_future,))?;

    let mut output = output.lock();
    match output.take() {
        Some(output) => output,
        None => Err(RayexecError::new("Missing output").into()),
    }
}

// TODO: Output could possibly be refcell.
fn spawn_python_future<'py, F, T>(
    py: Python<'py>,
    event_loop: Bound<'py, PyAny>,
    fut: F,
    output: Arc<Mutex<Option<Result<T>>>>,
) -> Result<Bound<'py, PyAny>>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + Send + 'static,
{
    let py_future = event_loop.call_method0("create_future")?;

    let py_fut1 = PyObject::from(py_future.clone());

    tokio_handle().spawn(async move {
        let result = fut.await;
        let mut output = output.lock();
        output.replace(result);

        Python::with_gil(move |py| {
            py_fut1.call_method1(py, "set_result", ("1",)).unwrap();
        });
    });

    Ok(py_future)
}

struct PyDoneCallback {}
