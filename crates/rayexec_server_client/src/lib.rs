//! Client and type definitions to connecting to a server.
//!
//! This exists a separate crate to make the dependency graph easier to manage.
//! Specifically the execution crate and binding crates (CLI, wasm) should import
//! this crate. The server crate should import this crate and the execution crate.
use url::Url;

pub const API_VERSION: usize = 1;

#[derive(Debug)]
pub struct HybridClient {
    /// Client used to connect to remote server.
    ///
    /// Note that this isn't using our `HttpClient` trait as that's specific to
    /// reading (and eventually writing) files. Adding in arbitrary requests
    /// would complicate the interface.
    ///
    /// However this means _some_ care needs to be take to ensure this can
    /// comple for wasm.
    client: reqwest::Client,

    /// URL of server we're "connected" to.
    url: Url,
}

impl HybridClient {
    pub fn new(url: Url) -> Self {
        HybridClient {
            client: reqwest::Client::default(),
            url,
        }
    }
}
