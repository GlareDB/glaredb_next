use bytes::Bytes;
use rayexec_error::{RayexecError, Result, ResultExt};
use std::fmt::Debug;
use tracing::debug;
use url::Url;

use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
    Future, Stream, StreamExt, TryStreamExt,
};
use reqwest::{
    header::{HeaderMap, CONTENT_LENGTH, RANGE},
    Body, IntoUrl, Method, StatusCode,
};

use crate::FileSource;

pub trait HttpClient: Sync + Send + Debug + Clone {
    type Response: HttpResponse + Send;
    type RequestFuture: Future<Output = Result<Self::Response>> + Send;

    fn request_with_body<U: IntoUrl, B: Into<Body>>(
        &self,
        method: Method,
        url: U,
        headers: HeaderMap,
        body: B,
    ) -> Self::RequestFuture;

    fn request<U: IntoUrl>(
        &self,
        method: Method,
        url: U,
        headers: HeaderMap,
    ) -> Self::RequestFuture;
}

pub trait HttpResponse {
    type BytesFuture: Future<Output = Result<Bytes>> + Send;
    type BytesStream: Stream<Item = Result<Bytes>> + Send;

    fn status(&self) -> StatusCode;
    fn headers(&self) -> &HeaderMap;
    fn bytes(self) -> Self::BytesFuture;
    fn bytes_stream(self) -> Self::BytesStream;
}

#[derive(Debug)]
pub struct HttpClientReader<C: HttpClient> {
    pub client: C,
    pub url: Url,
}

impl<C: HttpClient + 'static> FileSource for HttpClientReader<C> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        debug!(url = %self.url, %start, %len, "http reading range");

        let range = format_range_header(start, start + len - 1);
        let mut headers = HeaderMap::new();
        headers.insert(RANGE, range.try_into().unwrap());

        let fut = self.client.request(Method::GET, self.url.clone(), headers);

        Box::pin(async move {
            let resp = fut.await?;

            if resp.status() != StatusCode::PARTIAL_CONTENT {
                return Err(RayexecError::new("Server does not support range requests"));
            }

            resp.bytes().await.context("failed to get response body")
        })
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        debug!(url = %self.url, "http reading stream");

        let url = self.url.clone();
        let client = self.client.clone();

        let stream = stream::once(async move {
            let resp = client.request(Method::GET, url, HeaderMap::new()).await?;

            Ok::<_, RayexecError>(resp.bytes_stream())
        })
        .try_flatten();

        stream.boxed()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        debug!(url = %self.url, "http getting content length");

        let fut = self
            .client
            .request(Method::HEAD, self.url.clone(), HeaderMap::new());

        Box::pin(async move {
            let resp = fut.await?;

            if !resp.status().is_success() {
                return Err(RayexecError::new("Failed to get content-length"));
            }

            let len = match resp.headers().get(CONTENT_LENGTH) {
                Some(header) => header
                    .to_str()
                    .context("failed to convert to string")?
                    .parse::<usize>()
                    .context("failed to parse content length")?,
                None => return Err(RayexecError::new("Response missing content-length header")),
            };

            Ok(len)
        })
    }
}

fn format_range_header(start: usize, end: usize) -> String {
    format!("bytes={start}-{end}")
}
