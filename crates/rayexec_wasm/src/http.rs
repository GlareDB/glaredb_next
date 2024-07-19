use bytes::Bytes;
use futures::{
    future::{BoxFuture, FutureExt, TryFutureExt},
    stream::{BoxStream, StreamExt},
    Stream,
};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::{
    http::{HttpClient, HttpResponse, ReqwestClientReader},
    FileSource,
};
use reqwest::{header::HeaderMap, Body, IntoUrl, Method, StatusCode};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::debug;

#[derive(Debug, Clone)]
pub struct WasmHttpClient {
    client: reqwest::Client,
}

impl HttpClient for WasmHttpClient {
    type Response = WasmBoxingResponse;
    type RequestFuture = BoxFuture<'static, Result<Self::Response>>;

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
            .send()
            .map(|result| match result {
                Ok(resp) => Ok(WasmBoxingResponse(resp)),
                Err(e) => Err(RayexecError::with_source(
                    "Failed to make request",
                    Box::new(e),
                )),
            });

        unsafe { FakeSendFuture::new(Box::pin(fut)) }.boxed()
    }

    fn request<U: IntoUrl>(
        &self,
        method: Method,
        url: U,
        headers: HeaderMap,
    ) -> Self::RequestFuture {
        let fut = self
            .client
            .request(method, url)
            .headers(headers)
            .send()
            .map(|result| match result {
                Ok(resp) => Ok(WasmBoxingResponse(resp)),
                Err(e) => Err(RayexecError::with_source(
                    "Failed to make request",
                    Box::new(e),
                )),
            });

        unsafe { FakeSendFuture::new(Box::pin(fut)) }.boxed()
    }
}

#[derive(Debug)]
pub struct WasmBoxingResponse(pub reqwest::Response);

/// Same rationale as below for the fake send future/stream.
unsafe impl Send for WasmBoxingResponse {}

impl HttpResponse for WasmBoxingResponse {
    type BytesFuture = BoxFuture<'static, Result<Bytes>>;
    type BytesStream = BoxStream<'static, Result<Bytes>>;

    fn status(&self) -> StatusCode {
        self.0.status()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }

    fn bytes(self) -> Self::BytesFuture {
        let fut = Box::pin(self.0.bytes());
        let fut = unsafe { FakeSendFuture::new(fut) };
        fut.map(|r| r.context("failed to get byte response"))
            .boxed()
    }

    fn bytes_stream(self) -> Self::BytesStream {
        let stream = Box::pin(self.0.bytes_stream());
        let stream = unsafe { FakeSendStream::new(stream) };
        stream
            .map(|r| r.context("failed to get byte stream"))
            .boxed()
    }
}

#[derive(Debug)]
pub struct WrappedReqwestClientReader {
    pub inner: ReqwestClientReader,
}

impl FileSource for WrappedReqwestClientReader {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let fut = self.inner.read_range(start, len);
        let fut = unsafe { FakeSendFuture::new(Box::pin(fut)) };
        fut.boxed()
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        debug!(url = %self.inner.url, "http streaming (local stream)");

        // Similar to what we do for the "native" client, but boxes everything
        // local. Needed since no part of the stream can be Send in wasm, it's
        // not sufficient to just `boxed_local` the stream, the inner GET
        // requested needs to be local too.
        let fut =
            self.inner
                .client
                .get(self.inner.url.as_str())
                .send()
                .map(|result| match result {
                    Ok(resp) => Ok(resp
                        .bytes_stream()
                        .map(|result| result.context("failed to stream response"))
                        .boxed_local()),
                    Err(e) => Err(RayexecError::with_source(
                        "Failed to send GET request",
                        Box::new(e),
                    )),
                });

        let stream = fut.try_flatten_stream().boxed_local();

        FakeSendStream { stream }.boxed()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        let fut = self.inner.content_length();
        let fut = unsafe { FakeSendFuture::new(Box::pin(fut)) };
        fut.boxed()
    }
}

/// Implements Send for futures that aren't actually Send.
///
/// This is needed because the future that we get from reqwest when compiling
/// for wasm is a JsFuture which is not Send. However we make assumptions
/// everywhere in our execution engine that Futures are Send (due to using
/// BoxFuture). To avoid messing with Send vs not-Send, we can use this adapter
/// to unsafely make the JsFuture send.
///
/// This is safe in our case because we're operating on a single thread when
/// running in wasm, and so there's no actual place for our future to be sent
/// to.
///
/// Once we add in web workers for multi-threading, this will still hold because
/// web workers do not transparently share memory, and if we want to send
/// something from one worker to another, we need to do that explicitly via
/// message passing and some sort of serialization.
///
/// I believe there's a way for us to allow the ExecutionRuntime to specify if
/// the implemenation requires Send futures or not, but I (Sean) did not feel
/// like spending the time to figure that out in the initial implemenation.
#[derive(Debug)]
pub struct FakeSendFuture<O, F: Future<Output = O> + Unpin> {
    fut: F,
}

unsafe impl<O, F: Future<Output = O> + Unpin> Send for FakeSendFuture<O, F> {}

impl<O, F: Future<Output = O> + Unpin> FakeSendFuture<O, F> {
    pub unsafe fn new(fut: F) -> Self {
        FakeSendFuture { fut }
    }
}

impl<O, F: Future<Output = O> + Unpin> Future for FakeSendFuture<O, F> {
    type Output = O;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = &mut self.as_mut().fut;
        fut.poll_unpin(cx)
    }
}

/// Similar to `FakeSendFuture`, this unsafely makes a stream send.
#[derive(Debug)]
pub struct FakeSendStream<O, S: Stream<Item = O> + Unpin> {
    stream: S,
}

unsafe impl<O, S: Stream<Item = O> + Unpin> Send for FakeSendStream<O, S> {}

impl<O, S: Stream<Item = O> + Unpin> FakeSendStream<O, S> {
    pub unsafe fn new(stream: S) -> Self {
        FakeSendStream { stream }
    }
}

impl<O, S: Stream<Item = O> + Unpin> Stream for FakeSendStream<O, S> {
    type Item = O;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
