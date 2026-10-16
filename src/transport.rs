//! The wire seam: one request/response against one Node base URL.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::SdkError;

/// Result of one HTTP exchange, decoupled from the client library so
/// tests can script replies without a socket.
#[derive(Clone, Debug)]
pub struct TransportReply {
    pub status: u16,
    pub body: Value,
}

/// The wire seam: one request/response against one Node base URL.
/// Implementations must not retry internally — ambiguity handling is the
/// SDK's contract.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn request(
        &self,
        base: &str,
        method: &str,
        path: &str,
        body: Option<&Value>,
        api_key: &str,
    ) -> Result<TransportReply, SdkError>;
}

/// Production reqwest transport over rustls. `request_timeout` bounds
/// one exchange; the SDK, not the socket, owns retry policy.
pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    pub fn new() -> Result<Self, SdkError> {
        Self::with_timeout(Duration::from_secs(30))
    }

    /// Override the per-exchange timeout (bounded waits plus margin).
    pub fn with_timeout(timeout: Duration) -> Result<Self, SdkError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| SdkError::Transport(error.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn request(
        &self,
        base: &str,
        method: &str,
        path: &str,
        body: Option<&Value>,
        api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let builder = match method {
            "GET" => self.client.get(&url),
            _ => self.client.post(&url),
        }
        .bearer_auth(api_key);
        let builder = match body {
            Some(value) => builder.json(value),
            None => builder,
        };
        let response = builder
            .send()
            .await
            .map_err(|error| SdkError::Transport(error.to_string()))?;
        let status = response.status().as_u16();
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        Ok(TransportReply { status, body })
    }
}
