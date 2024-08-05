use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::http::{
    reqwest::{Method, Request, StatusCode},
    HttpClient, HttpResponse,
};
use std::fmt::Debug;
use url::Url;
use uuid::Uuid;

use super::stream::StreamId;

pub const API_VERSION: usize = 0;

pub const REMOTE_ENDPOINTS: Endpoints = Endpoints {
    healthz: "/healthz",
    rpc_hybrid_run: "/rpc/v0/hybrid/run",
    rpc_hybrid_push: "/rpc/v0/hybrid/push_batch",
    rpc_hybrid_pull: "/rpc/v0/hybrid/pull_batch",
};

#[derive(Debug)]
pub struct Endpoints {
    pub healthz: &'static str,
    pub rpc_hybrid_run: &'static str,
    pub rpc_hybrid_push: &'static str,
    pub rpc_hybrid_pull: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridConnectConfig {
    pub remote: Url,
}

#[derive(Debug)]
pub enum PullStatus {
    Batch(Batch),
    Pending,
    Finished,
}

#[derive(Debug)]
pub struct HybridClient<C: HttpClient> {
    url: Url,
    client: C,
}

impl<C: HttpClient> HybridClient<C> {
    pub async fn ping(&self) -> Result<()> {
        let url = self
            .url
            .join(REMOTE_ENDPOINTS.healthz)
            .context("failed to parse healthz url")?;
        let resp = self
            .client
            .do_request(Request::new(Method::GET, url))
            .await
            .context("failed to send request")?;

        if resp.status() != StatusCode::OK {
            return Err(RayexecError::new(format!(
                "Expected 200 from healthz, got {}",
                resp.status().as_u16()
            )));
        }

        Ok(())
    }

    pub async fn push(&self, stream_id: &StreamId, partition: usize, batch: Batch) -> Result<()> {
        unimplemented!()
    }

    pub async fn finalize(&self, stream_id: &StreamId, partition: usize) -> Result<()> {
        unimplemented!()
    }

    pub async fn pull(&self, stream_id: &StreamId, partition: usize) -> Result<PullStatus> {
        unimplemented!()
    }
}
