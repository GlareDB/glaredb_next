use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use rayexec_io::http::{reqwest::StatusCode, HttpClient};
use std::fmt::Debug;
use url::Url;
use uuid::Uuid;

use crate::{
    execution::executable::pipeline::ExecutablePartitionPipeline,
    logical::sql::binder::StatementWithBindData,
};

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

// pub trait HybridClient: Debug + Sync + Send {
//     fn ping(&self) -> BoxFuture<'_, Result<()>>;

//     fn remote_bind(
//         &self,
//         statement: StatementWithBindData,
//     ) -> BoxFuture<'_, Result<Vec<ExecutablePartitionPipeline>>>;

//     // TODO: batch enum (more?, done?), query id
//     fn pull(&self) -> BoxFuture<'_, Result<Option<Batch>>>;

//     // TODO: Query id
//     fn push(&self, batch: Batch) -> BoxFuture<'_, Result<()>>;
// }

#[derive(Debug)]
pub enum PullStatus {
    Batch(Batch),
    Pending,
    Finished,
}

#[derive(Debug)]
pub struct HybridClient<C: HttpClient> {
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
            .get(url)
            .send()
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

    pub async fn push(&self, query_id: &Uuid, partition: usize, batch: Batch) -> Result<()> {
        unimplemented!()
    }

    pub async fn finalize(&self, query_id: &Uuid, partition: usize) -> Result<()> {
        unimplemented!()
    }

    pub async fn pull(&self, query_id: &Uuid, partition: usize) -> Result<PullStatus> {
        unimplemented!()
    }
}
