use std::sync::Arc;

use axum::{extract::State, Json};
use rayexec_execution::{engine::Engine, logical::sql::binder::BoundStatement};
use serde::{Deserialize, Serialize};

use crate::errors::ServerResult;

/// State that's passed to all handlers.
#[derive(Debug)]
pub struct ServerState {
    /// Engine responsible for planning and executing queries.
    pub engine: Engine,
}

pub async fn healthz(State(_): State<Arc<ServerState>>) -> &'static str {
    "OK"
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanAstRpcRequest {
    ast: BoundStatement,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanAstRpcResponse {
    hello: String,
}

pub async fn plan_ast_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<PlanAstRpcRequest>,
) -> ServerResult<Json<PlanAstRpcResponse>> {
    Ok(PlanAstRpcResponse {
        hello: "hello".to_string(),
    }
    .into())
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PushBatchRcpRequest {
    // TODO: id
    ipc_data: Vec<u8>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PushBatchRpcResponse {}

pub async fn push_batch_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<PushBatchRcpRequest>,
) -> ServerResult<Json<PushBatchRpcResponse>> {
    unimplemented!()
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PullBatchRpcRequest {
    // TODO: id
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct PullBatchRpcResponse {
    ipc_data: Vec<u8>,
}

pub async fn pull_batch_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<PullBatchRpcRequest>,
) -> ServerResult<Json<PullBatchRpcResponse>> {
    unimplemented!()
}
