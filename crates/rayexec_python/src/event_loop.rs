use parking_lot::Mutex;
use pyo3::types::PyAnyMethods;
use rayexec_error::RayexecError;
use std::future::Future;
use std::sync::{Arc, OnceLock};

use pyo3::{pyclass, pymethods, Py, PyAny, Python};

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
    // Create a new event loop for this future.
    // TODO: I don't know if creating a new one for each future is good or not.
    let event_loop = py.import_bound("asyncio")?.call_method0("new_event_loop")?;

    // Output will contain the result of the (rust) future when it completes.
    //
    // TODO: Could be refcell.
    let output = Arc::new(Mutex::new(None));

    // Create a python future on this event loop.
    let py_future = event_loop.call_method0("create_future")?;
    py_future.call_method1("add_done_callback", (PyDoneCallback,))?;
    // Unbind the future from the GIL. Lets us send across threads.
    let py_future = py_future.unbind();

    // Unbind event loop from GIL, let's us send across threads.
    let event_loop = event_loop.unbind();

    spawn_python_future(
        event_loop.clone_ref(py),
        py_future.clone_ref(py),
        fut,
        output.clone(),
    )?;

    // Wait for the future to complete on the event loop.
    // TODO: Idk if this keeps the GIL or not.
    event_loop.call_method1(py, "run_until_complete", (py_future,))?;

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
        // Await the (rust) future and set the output.
        let result = fut.await;
        {
            let mut output = output.lock();
            output.replace(result);
        }

        Python::with_gil(move |py| {
            // Set a dummy result on the python future. Doesn't need an actual
            // value since we're just using it for signalling.
            py_fut.call_method1(py, "set_result", (true,)).unwrap();
            // Trigger the event loop to run. Passed a callback that does
            // nothing.
            event_loop
                .call_method1(py, "call_soon_threadsafe", (PyCallSoonCallback,))
                .unwrap();
        });
    });

    Ok(())
}

/// Callback for the done callback on the py future.
///
/// Doesn't do anything yet but should be used for cancellation.
#[pyclass]
#[derive(Debug, Clone, Copy)]
struct PyDoneCallback;

#[pymethods]
impl PyDoneCallback {
    fn __call__(&self, _py_fut: Py<PyAny>) -> Result<()> {
        Ok(())
    }
}

/// Callback for the `call_soon_threadsafe call`, doesn't do anything.
#[pyclass]
#[derive(Debug, Clone, Copy)]
struct PyCallSoonCallback;

#[pymethods]
impl PyCallSoonCallback {
    fn __call__(&self) -> Result<()> {
        Ok(())
    }
}
