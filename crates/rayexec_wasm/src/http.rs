use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use rayexec_error::Result;
use rayexec_io::{
    http::{HttpClient, HttpReader, ReqwestClient, ReqwestClientReader},
    AsyncReader,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use url::Url;

#[derive(Debug)]
pub struct WrappedReqwestClient {
    pub inner: ReqwestClient,
}

impl HttpClient for WrappedReqwestClient {
    fn reader(&self, url: Url) -> Box<dyn HttpReader> {
        Box::new(WrappedReqwestClientReader {
            inner: self.inner.reader(url),
        })
    }
}

#[derive(Debug)]
pub struct WrappedReqwestClientReader {
    pub inner: ReqwestClientReader,
}

impl AsyncReader for WrappedReqwestClientReader {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let fut = self.inner.read_range(start, len);
        let fut = unsafe { SendFutureNotReally::new(Box::pin(fut)) };
        fut.boxed()
    }
}

impl HttpReader for WrappedReqwestClientReader {
    fn content_length(&mut self) -> BoxFuture<Result<usize>> {
        let fut = self.inner.content_length();
        let fut = unsafe { SendFutureNotReally::new(Box::pin(fut)) };
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
struct SendFutureNotReally<O, F: Future<Output = O> + Unpin> {
    fut: F,
}

unsafe impl<O, F: Future<Output = O> + Unpin> Send for SendFutureNotReally<O, F> {}

impl<O, F: Future<Output = O> + Unpin> SendFutureNotReally<O, F> {
    pub unsafe fn new(fut: F) -> Self {
        SendFutureNotReally { fut }
    }
}

impl<O, F: Future<Output = O> + Unpin> Future for SendFutureNotReally<O, F> {
    type Output = O;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = &mut self.as_mut().fut;
        fut.poll_unpin(cx)
    }
}
