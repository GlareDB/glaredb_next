use crate::{logical::sql::binder::bind_data::BindData, proto::DatabaseProtoConv};
use rayexec_bullet::batch::Batch;
use rayexec_error::{OptionExt, RayexecError, Result, ResultExt};
use rayexec_io::http::{
    reqwest::{Method, Request, StatusCode},
    HttpClient, HttpResponse,
};
use rayexec_proto::prost::Message;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use url::Url;
use uuid::Uuid;

use crate::{database::DatabaseContext, logical::sql::binder::BoundStatement};

use super::stream::StreamId;

pub const API_VERSION: usize = 0;

pub const REMOTE_ENDPOINTS: Endpoints = Endpoints {
    healthz: "/healthz",
    rpc_hybrid_plan: "/rpc/v0/hybrid/plan",
    rpc_hybrid_execute: "/rpc/v0/hybrid/execute",
    rpc_hybrid_push: "/rpc/v0/hybrid/push_batch",
    rpc_hybrid_finalize: "/rpc/v0/hybrid/finalize",
    rpc_hybrid_pull: "/rpc/v0/hybrid/pull_batch",
};

#[derive(Debug)]
pub struct Endpoints {
    pub healthz: &'static str,
    pub rpc_hybrid_plan: &'static str,
    pub rpc_hybrid_execute: &'static str,
    pub rpc_hybrid_push: &'static str,
    pub rpc_hybrid_finalize: &'static str,
    pub rpc_hybrid_pull: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HybridPlanRequest {
    /// The sql statement we're planning.
    ///
    /// This includes partially bound items that reference the things in the
    /// bind data.
    pub statement: BoundStatement,
    pub bind_data: BindData,
}

impl DatabaseProtoConv for HybridPlanRequest {
    type ProtoType = rayexec_proto::generated::hybrid::PlanRequest;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        let statement =
            serde_json::to_vec(&self.statement).context("failed to encode statement")?;
        Ok(Self::ProtoType {
            bound_statement_json: statement,
            bind_data: Some(self.bind_data.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let statement = serde_json::from_slice(&proto.bound_statement_json)
            .context("failed to decode statement")?;
        Ok(Self {
            statement,
            bind_data: BindData::from_proto_ctx(proto.bind_data.required("bind_data")?, context)?,
        })
    }
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
