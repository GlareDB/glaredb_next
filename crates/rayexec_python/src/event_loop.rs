use parking_lot::Mutex;
use pyo3::types::PyAnyMethods;
use rayexec_error::RayexecError;
use std::sync::{Arc, OnceLock};
use std::{cell::OnceCell, future::Future};

use pyo3::{pyclass, pymethods, Bound, IntoPy, Py, PyAny, PyObject, Python};

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

    let py_future = event_loop.call_method0("create_future")?.unbind();
    let event_loop = event_loop.unbind();

    spawn_python_future(
        event_loop.clone_ref(py),
        py_future.clone_ref(py),
        fut,
        output.clone(),
    )?;

    println!("BEFORE");
    event_loop.call_method1(py, "run_until_complete", (py_future,))?;
    println!("AFTER");

    let mut output = output.lock();
    match output.take() {
        Some(output) => output,
        None => Err(RayexecError::new("Missing output").into()),
    }
}

// TODO: Output could possibly be refcell.
fn spawn_python_future<'py, F, T>(
    event_loop: Py<PyAny>,
    py_fut: Py<PyAny>,
    fut: F,
    output: Arc<Mutex<Option<Result<T>>>>,
) -> Result<()>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    tokio_handle().spawn(async move {
        println!("SPAWNED");
        let result = fut.await;
        println!("GOT RESULTS");

        {
            let mut output = output.lock();
            output.replace(result);
        }

        println!("LOCKED");

        Python::with_gil(move |py| {
            py_fut.call_method1(py, "set_result", ("1",)).unwrap();
            event_loop
                .call_method1(py, "call_soon_threadsafe", (PyCallSoonCallback,))
                .unwrap();
        });

        println!("RESULTS SET");
    });

    Ok(())
}

struct PyDoneCallback {}

/// Callback for the `call_soon_threadsafe call`, doesn't do anything.
#[pyclass]
struct PyCallSoonCallback;

#[pymethods]
impl PyCallSoonCallback {
    fn __call__(&self) -> Result<()> {
        Ok(())
    }
}
