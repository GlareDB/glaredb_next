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
use tracing::{debug, trace};
use url::Url;

#[derive(Debug)]
pub struct WrappedReqwestClient {
    pub inner: ReqwestClient,
}

impl HttpClient for WrappedReqwestClient {
    fn reader(&self, url: Url) -> Box<dyn HttpReader> {
        Box::new(WrappedReqwestClientReader {
            inner: self.inner.reader_inner(url),
        })
    }
}

#[derive(Debug)]
pub struct WrappedReqwestClientReader {
    pub inner: ReqwestClientReader,
}

impl AsyncReader for WrappedReqwestClientReader {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let fut = self.inner.read_range_inner(start, len);
        let fut = unsafe { SendFutureNotReally::new(Box::pin(fut)) };
        fut.boxed()
    }
}

impl HttpReader for WrappedReqwestClientReader {
    fn content_length(&mut self) -> BoxFuture<Result<usize>> {
        let fut = self.inner.content_length_inner();
        let fut = unsafe { SendFutureNotReally::new(Box::pin(fut)) };
        fut.boxed()
    }
}

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
