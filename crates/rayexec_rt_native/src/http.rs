use std::{
    convert,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{
    future::{BoxFuture, FutureExt, TryFutureExt},
    stream::{BoxStream, StreamExt},
    Future, Stream,
};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::{
    http::{BoxingResponse, HttpClient, ReqwestClientReader},
    FileSource,
};
use reqwest::{header::HeaderMap, Body, IntoUrl, Method};
use tokio::task::JoinHandle;
use tracing::debug;

/// Wrapper around a reqwest client that ensures are request are done in a tokio
/// context.
#[derive(Debug, Clone)]
pub struct TokioWrappedHttpClient {
    client: reqwest::Client,
    handle: tokio::runtime::Handle,
}

impl TokioWrappedHttpClient {
    pub fn new(client: reqwest::Client, handle: tokio::runtime::Handle) -> Self {
        TokioWrappedHttpClient { client, handle }
    }
}

impl HttpClient for TokioWrappedHttpClient {
    type Response = BoxingResponse;
    type RequestFuture = ResponseJoinHandle;

    fn request_with_body<U: IntoUrl, B: Into<Body>>(
        &self,
        method: Method,
        url: U,
        headers: HeaderMap,
        body: B,
    ) -> Self::RequestFuture {
        let fut = self
            .client
            .request(method, url)
            .headers(headers)
            .body(body)
            .send();
        let join_handle = self.handle.spawn(async move {
            let resp = fut.await.context("Failed to send request")?;
            Ok(BoxingResponse(resp))
        });

        ResponseJoinHandle { join_handle }
    }

    fn request<U: IntoUrl>(
        &self,
        method: Method,
        url: U,
        headers: HeaderMap,
    ) -> Self::RequestFuture {
        let fut = self.client.request(method, url).headers(headers).send();
        let join_handle = self.handle.spawn(async move {
            let resp = fut.await.context("Failed to send request")?;
            Ok(BoxingResponse(resp))
        });

        ResponseJoinHandle { join_handle }
    }
}

/// Wrapper around a tokio join handle waiting on a boxed response.
pub struct ResponseJoinHandle {
    join_handle: JoinHandle<Result<BoxingResponse>>,
}

impl Future for ResponseJoinHandle {
    type Output = Result<BoxingResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.join_handle.poll_unpin(cx) {
            Poll::Ready(Err(_)) => Poll::Ready(Err(RayexecError::new("tokio join error"))),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(Ok(b))) => Poll::Ready(Ok(b)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct WrappedReqwestClientReader {
    pub inner: ReqwestClientReader,
    pub handle: tokio::runtime::Handle,
}

impl WrappedReqwestClientReader {
    async fn read_range_inner(&mut self, start: usize, len: usize) -> Result<Bytes> {
        let mut inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.read_range(start, len).await })
            .await
            .context("join error")?
    }

    async fn create_read_stream(
        handle: tokio::runtime::Handle,
        inner: ReqwestClientReader,
    ) -> Result<impl Stream<Item = Result<Bytes>>> {
        let response = handle
            .spawn(async move {
                inner
                    .client
                    .get(inner.url.as_str())
                    .send()
                    .await
                    .context("Make get request")
            })
            .await
            .context("join handle")
            .and_then(convert::identity)?;

        let stream = response
            .bytes_stream()
            .map(|result| result.context("failed to stream response"));

        Ok(stream)
    }
}

impl FileSource for WrappedReqwestClientReader {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.read_range_inner(start, len).boxed()
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        debug!(url = %self.inner.url, "http streaming (send stream)");

        // Folds the initial GET request into the stream.
        let inner = self.inner.clone();
        let fut = Self::create_read_stream(self.handle.clone(), inner);

        fut.try_flatten_stream().boxed()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        self.inner.content_length().boxed()
    }
}
