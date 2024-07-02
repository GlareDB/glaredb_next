use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use rayexec_error::{Result, ResultExt};
use rayexec_execution::{
    execution::query_graph::QueryGraph,
    runtime::{dump::QueryDump, ErrorSink, ExecutionRuntime, QueryHandle},
};
use rayexec_io::http::{HttpClient, ReqwestClient};
use tracing::{debug, trace};

use crate::http::WrappedReqwestClient;

#[derive(Debug)]
pub struct WasmExecutionRuntime {
    runtime: tokio::runtime::Runtime,
}

impl WasmExecutionRuntime {
    pub fn try_new() -> Result<Self> {
        debug!("creating wasm execution runtime");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .thread_name("rayexec_wasm")
            .build()
            .context("Failed to build tokio runtime")?;

        Ok(WasmExecutionRuntime { runtime })
    }
}

impl ExecutionRuntime for WasmExecutionRuntime {
    fn spawn_query_graph(
        &self,
        query_graph: QueryGraph,
        errors: Arc<dyn ErrorSink>,
    ) -> Box<dyn QueryHandle> {
        debug!("spawning query graph on wasm runtime");

        self.serial_execute(query_graph, errors);

        Box::new(WasmQueryHandle {})
    }

    fn tokio_handle(&self) -> Option<tokio::runtime::Handle> {
        None
    }

    fn http_client(&self) -> Result<Arc<dyn HttpClient>> {
        debug!("creating http client");
        Ok(Arc::new(WrappedReqwestClient {
            inner: ReqwestClient::new(),
        }))
    }
}

impl WasmExecutionRuntime {
    fn serial_execute(&self, query_graph: QueryGraph, errors: Arc<dyn ErrorSink>) {
        let mut pipelines: VecDeque<_> = query_graph.into_partition_pipeline_iter().collect();
        while pipelines.len() != 0 {
            let mut pipeline = pipelines.pop_front().unwrap();

            let waker: Waker = Arc::new(WasmWaker {}).into();
            let mut cx = Context::from_waker(&waker);
            loop {
                trace!("poll execute loop");
                match pipeline.poll_execute(&mut cx) {
                    Poll::Ready(Some(Ok(()))) => {
                        continue;
                    }
                    Poll::Ready(Some(Err(e))) => {
                        errors.push_error(e);
                        return;
                    }
                    Poll::Pending => {
                        pipelines.push_back(pipeline);
                        break;
                    }
                    Poll::Ready(None) => {
                        break;
                    }
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
struct WasmWaker {}

impl Wake for WasmWaker {
    fn wake(self: Arc<Self>) {}

    fn wake_by_ref(self: &Arc<Self>) {}
}
