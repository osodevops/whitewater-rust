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

/// Result of one binary HTTP exchange (e.g. `Accept:
/// application/vnd.whitewater.frames` on fetch): raw body bytes; a
/// non-2xx status still carries the server's JSON error body.
#[derive(Clone, Debug)]
pub struct TransportBytesReply {
    pub status: u16,
    pub body: Vec<u8>,
    /// Lowercased response headers — session metadata rides headers on
    /// the binary fetch wire (reader pacing advice, acked cursor).
    pub headers: Vec<(String, String)>,
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

    /// Binary-body exchange — `Accept: application/vnd.whitewater.frames`
    /// on the request, raw bytes on success, the server's JSON error
    /// body otherwise. The default refuses so JSON-only test transports
    /// keep compiling unchanged.
    async fn request_binary(
        &self,
        _base: &str,
        _path: &str,
        _body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportBytesReply, SdkError> {
        Err(SdkError::Refused(
            "binary fetch is not supported by this Transport".to_owned(),
        ))
    }

    /// Fetch one binary records page. `HttpTransport` streams and
    /// decodes incrementally; the default keeps scripted transports
    /// working by falling back to `request_binary` + whole-buffer parse.
    /// `on_header` receives the page's `next_cursor` as soon as the
    /// response header is parsed — `HttpTransport` fires it mid-stream so
    /// cursor-chained prefetch overlaps the body; the default fires it
    /// after the whole page is parsed.
    async fn request_frame_page(
        &self,
        base: &str,
        path: &str,
        body: Option<&Value>,
        api_key: &str,
        on_header: Option<tokio::sync::oneshot::Sender<Option<String>>>,
    ) -> Result<TransportFramePageReply, SdkError> {
        let reply = self.request_binary(base, path, body, api_key).await?;
        if (200..300).contains(&reply.status) {
            let page = crate::frame::parse_frame_page(&reply.body)?;
            if let Some(tx) = on_header {
                let _ = tx.send(page.1.clone());
            }
            Ok(TransportFramePageReply {
                status: reply.status,
                page: Some(page),
                body: Vec::new(),
                headers: reply.headers,
            })
        } else {
            Ok(TransportFramePageReply {
                status: reply.status,
                page: None,
                body: reply.body,
                headers: reply.headers,
            })
        }
    }

    /// GET variant of [`Transport::request_frame_page`] for the
    /// `/v1/feeds/records` history walk. Refuses by default so
    /// JSON-only scripted transports keep compiling.
    async fn request_frame_page_get(
        &self,
        _base: &str,
        _path: &str,
        _api_key: &str,
    ) -> Result<TransportFramePageReply, SdkError> {
        Err(SdkError::Refused(
            "binary GET fetch is not supported by this Transport".to_owned(),
        ))
    }

    /// Member-fetch wire variant (`Accept:
    /// application/vnd.whitewater.member-frames`): the body is a
    /// `u32 meta_len` + metadata JSON prelude followed by the standard
    /// frame page. Refuses by default so scripted transports fall back
    /// to the JSON request path.
    async fn request_meta_frame_page(
        &self,
        _base: &str,
        _path: &str,
        _body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportMetaFramePageReply, SdkError> {
        Err(SdkError::Refused(
            "meta frame pages are not supported by this Transport".to_owned(),
        ))
    }

    async fn request_binary_body(
        &self,
        _base: &str,
        _path: &str,
        _body: &[u8],
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        Err(SdkError::Refused(
            "binary append is not supported by this Transport".to_owned(),
        ))
    }
}

/// One frame-page exchange: the status line plus, on success, the
/// incrementally decoded page; on error the raw JSON body. `headers`
/// carries the response's lowercased header pairs — session metadata
/// lives there on the binary wire.
pub struct TransportFramePageReply {
    pub status: u16,
    pub page: Option<(Vec<crate::model::Record>, Option<String>)>,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

/// One member-fetch exchange: a 2xx `member-frames` reply yields the
/// decoded metadata prelude plus the frame page; a 2xx JSON reply
/// (server ignored the Accept) lands in `meta` verbatim instead, and a
/// non-2xx reply carries the raw JSON error body.
pub struct TransportMetaFramePageReply {
    pub status: u16,
    /// Decoded metadata prelude — or the whole JSON response body when
    /// the serving Node answered JSON on a binary Accept.
    pub meta: Option<Value>,
    /// The frame page tail; `next_cursor` carries the fetch `cursor`.
    pub page: Option<(Vec<crate::model::Record>, Option<String>)>,
    /// Raw error body on non-2xx statuses.
    pub body: Vec<u8>,
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
            .tcp_nodelay(true)
            .pool_max_idle_per_host(64)
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
        let response = builder.send().await.map_err(|error| {
            if error.is_timeout() {
                SdkError::Transport(format!("timeout: {url}"))
            } else {
                SdkError::Transport(error.to_string())
            }
        })?;
        let status = response.status().as_u16();
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        Ok(TransportReply { status, body })
    }

    async fn request_binary(
        &self,
        base: &str,
        path: &str,
        body: Option<&Value>,
        api_key: &str,
    ) -> Result<TransportBytesReply, SdkError> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let builder = self
            .client
            .post(&url)
            .bearer_auth(api_key)
            .header(reqwest::header::ACCEPT, "application/vnd.whitewater.frames");
        let builder = match body {
            Some(value) => builder.json(value),
            None => builder,
        };
        let response = builder.send().await.map_err(|error| {
            if error.is_timeout() {
                SdkError::Transport(format!("timeout: {url}"))
            } else {
                SdkError::Transport(error.to_string())
            }
        })?;
        let status = response.status().as_u16();
        let headers = response_headers(&response);
        let body = response
            .bytes()
            .await
            .map_err(|error| SdkError::Transport(error.to_string()))?
            .to_vec();
        Ok(TransportBytesReply {
            status,
            body,
            headers,
        })
    }

    async fn request_binary_body(
        &self,
        base: &str,
        path: &str,
        body: &[u8],
        api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let response = self
            .client
            .post(&url)
            .bearer_auth(api_key)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/vnd.whitewater.frames",
            )
            .body(body.to_vec())
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    SdkError::Transport(format!("timeout: {url}"))
                } else {
                    SdkError::Transport(error.to_string())
                }
            })?;
        let status = response.status().as_u16();
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        Ok(TransportReply { status, body })
    }

    async fn request_frame_page(
        &self,
        base: &str,
        path: &str,
        body: Option<&Value>,
        api_key: &str,
        on_header: Option<tokio::sync::oneshot::Sender<Option<String>>>,
    ) -> Result<TransportFramePageReply, SdkError> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let builder = self
            .client
            .post(&url)
            .bearer_auth(api_key)
            .header(reqwest::header::ACCEPT, "application/vnd.whitewater.frames");
        let builder = match body {
            Some(value) => builder.json(value),
            None => builder,
        };
        let response = builder.send().await.map_err(|error| {
            if error.is_timeout() {
                SdkError::Transport(format!("timeout: {url}"))
            } else {
                SdkError::Transport(error.to_string())
            }
        })?;
        frame_page_from_response(response, on_header).await
    }

    async fn request_frame_page_get(
        &self,
        base: &str,
        path: &str,
        api_key: &str,
    ) -> Result<TransportFramePageReply, SdkError> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let response = self
            .client
            .get(&url)
            .bearer_auth(api_key)
            .header(reqwest::header::ACCEPT, "application/vnd.whitewater.frames")
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    SdkError::Transport(format!("timeout: {url}"))
                } else {
                    SdkError::Transport(error.to_string())
                }
            })?;
        frame_page_from_response(response, None).await
    }

    async fn request_meta_frame_page(
        &self,
        base: &str,
        path: &str,
        body: Option<&Value>,
        api_key: &str,
    ) -> Result<TransportMetaFramePageReply, SdkError> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let builder = self.client.post(&url).bearer_auth(api_key).header(
            reqwest::header::ACCEPT,
            "application/vnd.whitewater.member-frames",
        );
        let builder = match body {
            Some(value) => builder.json(value),
            None => builder,
        };
        let response = builder.send().await.map_err(|error| {
            if error.is_timeout() {
                SdkError::Transport(format!("timeout: {url}"))
            } else {
                SdkError::Transport(error.to_string())
            }
        })?;
        let status = response.status().as_u16();
        let member_frames = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("member-frames"));
        let body = response
            .bytes()
            .await
            .map_err(|error| SdkError::Transport(error.to_string()))?
            .to_vec();
        if !(200..300).contains(&status) {
            return Ok(TransportMetaFramePageReply {
                status,
                meta: None,
                page: None,
                body,
            });
        }
        if !member_frames {
            // A Node that does not know the member-frames type answers
            // its ordinary JSON response: surface it as `meta` so the
            // caller decodes the same fields either way.
            return Ok(TransportMetaFramePageReply {
                status,
                meta: serde_json::from_slice(&body).ok(),
                page: None,
                body: Vec::new(),
            });
        }
        // `u32le meta_len` + metadata JSON + standard frame page.
        let meta_len = body
            .get(..4)
            .map(|prefix| u32::from_le_bytes(prefix.try_into().expect("4 bytes")) as usize)
            .filter(|len| body.len() >= 4 + len);
        let Some(meta_len) = meta_len else {
            return Err(SdkError::Protocol(
                "member fetch page corrupt: truncated metadata prelude".to_owned(),
            ));
        };
        let meta: Value = serde_json::from_slice(&body[4..4 + meta_len])
            .map_err(|error| SdkError::Protocol(format!("member metadata: {error}")))?;
        let page = crate::frame::parse_frame_page(&body[4 + meta_len..])?;
        Ok(TransportMetaFramePageReply {
            status,
            meta: Some(meta),
            page: Some(page),
            body: Vec::new(),
        })
    }
}

/// Lowercased response header pairs for the binary-wire metadata
/// (`x-whitewater-*`) fetch replies carry.
fn response_headers(response: &reqwest::Response) -> Vec<(String, String)> {
    response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect()
}

/// Shared streaming frame-page decode for the POST and GET variants:
/// records parse as chunks land, `on_header` fires when `next_cursor`
/// completes. A 2xx reply whose Content-Type is not the frames type
/// keeps its raw body instead — the caller falls back to the JSON
/// shape it carries.
async fn frame_page_from_response(
    response: reqwest::Response,
    on_header: Option<tokio::sync::oneshot::Sender<Option<String>>>,
) -> Result<TransportFramePageReply, SdkError> {
    let status = response.status().as_u16();
    let headers = response_headers(&response);
    let is_frames = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("application/vnd.whitewater.frames"));
    if !(200..300).contains(&status) || !is_frames {
        let body = response
            .bytes()
            .await
            .map_err(|error| SdkError::Transport(error.to_string()))?
            .to_vec();
        return Ok(TransportFramePageReply {
            status,
            page: None,
            body,
            headers,
        });
    }
    // Incremental decode: each record parses as soon as its bytes
    // land, so decode overlaps the transfer. The next_cursor is
    // delivered the moment the header completes so a pipelined
    // caller can launch the next page now.
    let mut parser = crate::frame::FramePageParser::new();
    let mut on_header = on_header;
    let mut response = response;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| SdkError::Transport(error.to_string()))?
    {
        parser.feed(&chunk)?;
        if on_header.is_some() && parser.header_ready() {
            if let Some(tx) = on_header.take() {
                let _ = tx.send(parser.next_cursor());
            }
        }
    }
    let page = parser.finish()?;
    Ok(TransportFramePageReply {
        status,
        page: Some(page),
        body: Vec::new(),
        headers,
    })
}
