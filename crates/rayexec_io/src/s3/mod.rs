pub mod credentials;

use bytes::Bytes;
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use reqwest::{
    header::{CONTENT_LENGTH, RANGE},
    Method, Request, StatusCode,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    http::{format_range_header, HttpClient, HttpResponse},
    FileSource,
};

const AWS_ENDPOINT: &str = "amazonaws.com";

/// A location to a single object in S3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Location {
    pub url: Url,
}

impl S3Location {
    pub fn is_s3_location(url: &Url) -> bool {
        // Very sophisticated.
        // TODO: Extend to support https schemas with aws endpoint.
        url.scheme() == "s3"
    }

    pub fn from_url(url: Url, region: &str) -> Result<S3Location> {
        match url.scheme() {
            "s3" => {
                let bucket = match url.host() {
                    Some(url::Host::Domain(host)) => host,
                    Some(_) => return Err(RayexecError::new("Unexpected host")),
                    None => return Err(RayexecError::new("Missing host")),
                };

                let object = url.path(); // Should include leading slash.
                let formatted = format!("https://{bucket}.{region}.{AWS_ENDPOINT}{object}");
                let url = Url::parse(&formatted)
                    .context_fn(|| format!("Failed to parse '{formatted}' into url"))?;

                Ok(S3Location { url })
            }
            "https" => {
                not_implemented!("non-vanity s3 urls")
            }
            scheme => Err(RayexecError::new(format!(
                "Invalid schema for s3 location: {scheme}"
            ))),
        }
    }
}

#[derive(Debug)]
pub struct S3Reader<C: HttpClient> {
    client: C,
    location: S3Location,
}

impl<C: HttpClient + 'static> S3Reader<C> {
    pub fn new(client: C, location: S3Location) -> Self {
        S3Reader { client, location }
    }
}

impl<C: HttpClient + 'static> FileSource for S3Reader<C> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let range = format_range_header(start, start + len - 1);

        let mut request = Request::new(Method::GET, self.location.url.clone());
        request
            .headers_mut()
            .insert(RANGE, range.try_into().unwrap());

        let fut = self.client.do_request(request);

        Box::pin(async move {
            let resp = fut.await?;

            if resp.status() != StatusCode::PARTIAL_CONTENT {
                return Err(RayexecError::new("Server does not support range requests"));
            }

            resp.bytes().await.context("failed to get response body")
        })
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        let client = self.client.clone();
        let req = Request::new(Method::GET, self.location.url.clone());

        let stream = stream::once(async move {
            let resp = client.do_request(req).await?;

            Ok::<_, RayexecError>(resp.bytes_stream())
        })
        .try_flatten();

        stream.boxed()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        let fut = self
            .client
            .do_request(Request::new(Method::GET, self.location.url.clone()));

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_valid_location() {
        let expected = Url::parse("https://my_bucket.us-east1.amazonaws.com/my/object").unwrap();
        let location =
            S3Location::from_url(Url::parse("s3://my_bucket/my/object").unwrap(), "us-east1")
                .unwrap();
        assert_eq!(expected, location.url)
    }

    #[test]
    fn parse_s3_invalid_location() {
        S3Location::from_url(Url::parse("gs://my_bucket/my/object").unwrap(), "us-east1")
            .unwrap_err();
    }
}
