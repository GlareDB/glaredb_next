use futures::stream::{self, BoxStream};
use parking_lot::Mutex;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_execution::{
    execution::{pipeline::PartitionPipeline, query_graph::QueryGraph},
    runtime::{dump::QueryDump, ErrorSink, ExecutionRuntime, QueryHandle},
};
use rayexec_io::{
    http::HttpClientReader,
    location::{AccessConfig, FileLocation},
    memory::MemoryFileSystem,
    s3::S3Reader,
    FileProvider, FileSink, FileSource,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};
use tracing::debug;
use wasm_bindgen_futures::spawn_local;

use crate::http::WasmHttpClient;

/// Execution runtime for wasm.
///
/// This implementation works on a single thread which each pipeline task being
/// spawned local to the thread (using js promises under the hood).
#[derive(Debug)]
pub struct WasmExecutionRuntime {
    // TODO: Remove Arc? Arc already used internally for the memory fs.
    pub(crate) fs: Arc<MemoryFileSystem>,
}

impl WasmExecutionRuntime {
    pub fn try_new() -> Result<Self> {
        debug!("creating wasm execution runtime");
        Ok(WasmExecutionRuntime {
            fs: Arc::new(MemoryFileSystem::default()),
        })
    }
}

impl ExecutionRuntime for WasmExecutionRuntime {
    fn spawn_query_graph(
        &self,
        query_graph: QueryGraph,
        errors: Arc<dyn ErrorSink>,
    ) -> Box<dyn QueryHandle> {
        debug!("spawning query graph on wasm runtime");

        let states: Vec<_> = query_graph
            .into_partition_pipeline_iter()
            .map(|pipeline| WasmTaskState {
                errors: errors.clone(),
                pipeline: Arc::new(Mutex::new(pipeline)),
            })
            .collect();

        // TODO: Put references into query handle to allow canceling.

        for state in states {
            spawn_local(async move { state.execute() })
        }

        Box::new(WasmQueryHandle {})
    }

    fn tokio_handle(&self) -> Option<tokio::runtime::Handle> {
        None
    }

    fn file_provider(&self) -> Arc<dyn FileProvider> {
        Arc::new(WasmFileProvider {
            fs: self.fs.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WasmFileProvider {
    fs: Arc<MemoryFileSystem>,
}

impl FileProvider for WasmFileProvider {
    fn file_source(
        &self,
        location: FileLocation,
        config: &AccessConfig,
    ) -> Result<Box<dyn FileSource>> {
        match location {
            FileLocation::Url(url) => {
                let client = WasmHttpClient::new(reqwest::Client::default());
                Ok(Box::new(HttpClientReader::new(client, url)))
            }
            FileLocation::Path(path) => self.fs.file_source(&path),
        }
    }

    fn file_sink(
        &self,
        location: FileLocation,
        config: &AccessConfig,
    ) -> Result<Box<dyn FileSink>> {
        match location {
            FileLocation::Url(_url) => not_implemented!("http sink wasm"),
            FileLocation::Path(path) => self.fs.file_sink(&path),
        }
    }

    fn list_prefix(
        &self,
        prefix: FileLocation,
        config: &AccessConfig,
    ) -> BoxStream<'static, Result<Vec<String>>> {
        match prefix {
            FileLocation::Url(_) => Box::pin(stream::once(async move {
                Err(RayexecError::new("Cannot list for http file sources"))
            })),
            FileLocation::Path(_) => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone)]
struct WasmTaskState {
    errors: Arc<dyn ErrorSink>,
    pipeline: Arc<Mutex<PartitionPipeline>>,
}

impl WasmTaskState {
    fn execute(&self) {
        let state = self.clone();
        let waker: Waker = Arc::new(WasmWaker { state }).into();
        let mut cx = Context::from_waker(&waker);

        let mut pipeline = self.pipeline.lock();
        loop {
            match pipeline.poll_execute(&mut cx) {
                Poll::Ready(Some(Ok(()))) => {
                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    self.errors.push_error(e);
                    return;
                }
                Poll::Pending => {
                    return;
                }
                Poll::Ready(None) => {
                    return;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct WasmQueryHandle {}

impl QueryHandle for WasmQueryHandle {
    fn cancel(&self) {
        // TODO
    }

    fn dump(&self) -> QueryDump {
        // TODO
        QueryDump {
            pipelines: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
struct WasmWaker {
    state: WasmTaskState,
}

impl Wake for WasmWaker {
    fn wake_by_ref(self: &Arc<Self>) {
        self.clone().wake()
    }

    fn wake(self: Arc<Self>) {
        spawn_local(async move { self.state.execute() })
    }
}
