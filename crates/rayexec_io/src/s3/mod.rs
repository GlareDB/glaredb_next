pub mod credentials;

use reqwest::header::HeaderMap;

use crate::http::{HttpClient, HttpClientReader};

pub struct S3Reader<C: HttpClient> {
    client: HttpClientReader<C>,
}
