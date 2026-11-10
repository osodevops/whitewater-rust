//! The bound endpoint pool: one `Client` spreads calls across any set
//! of Nodes — owner routing is a server detail that never crosses this
//! boundary.

use std::sync::Arc;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::codec::Codec;
use crate::consumer::ReaderStart;
use crate::domain::{Domain, FeedSchema, ReaderBuilder, WriterBuilder};
use crate::error::{map_error, SdkError};
use crate::feed::Feed;
use crate::member::Member;
use crate::model::{
    decode_record, urlencode, Record, TemporaryFetch, TemporaryPage, TransportLimits,
};
use crate::operations::{EnrichMiss, PipeOperation};
use crate::pipe::{decode_pipes, Pipe, PipeInfo};
use crate::reader::Reader;
use crate::store::{StateStore, StateStoreSpec};
use crate::transport::{
    HttpTransport, Transport, TransportFramePageReply, TransportMetaFramePageReply, TransportReply,
};
use crate::typed::{decode_index_page, wire_typed, IndexPage, IndexQuery};
use crate::typed_store::Store;
use crate::writer::Writer;

const SPEC_PROTOCOL_MAJOR: u64 = 1;

/// One bound Node endpoint the client may target; any Node serves any
/// request because owner routing is a server detail. Calls spread across
/// the pool and hop on transport failure.
#[derive(Clone)]
pub struct Client {
    transport: Arc<dyn Transport>,
    bases: Vec<String>,
    api_key: String,
    pub(crate) use_compression: bool,
    cursor: Arc<std::sync::atomic::AtomicUsize>,
}

impl Client {
    /// Bind a transport and Node pool. Custom `Transport`
    /// implementations plug in here; `Client::http` is the production
    /// shortcut.
    pub fn new(
        transport: Arc<dyn Transport>,
        bases: Vec<String>,
        api_key: impl Into<String>,
    ) -> Result<Self, SdkError> {
        if bases.is_empty() {
            return Err(SdkError::Refused(
                "Client needs at least one Node base URL".to_owned(),
            ));
        }
        Ok(Self {
            transport,
            bases,
            api_key: api_key.into(),
            use_compression: false,
            cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    /// The production client: `HttpTransport` over the given Node pool.
    pub fn http(bases: Vec<String>, api_key: impl Into<String>) -> Result<Self, SdkError> {
        Self::new(Arc::new(HttpTransport::new()?), bases, api_key)
    }

    /// The spec's per-append compression preference (stored/replicated
    /// record frames only; wire bodies are unchanged).
    pub fn use_compression(mut self, enabled: bool) -> Self {
        self.use_compression = enabled;
        self
    }

    /// A clone sharing transport, endpoint pool, and compression
    /// preference but carrying a different API key — the conformance
    /// runner uses it for scenario-level credential overrides.
    pub fn with_api_key(&self, api_key: impl Into<String>) -> Self {
        Self {
            transport: self.transport.clone(),
            bases: self.bases.clone(),
            api_key: api_key.into(),
            use_compression: self.use_compression,
            cursor: self.cursor.clone(),
        }
    }

    fn base(&self) -> &str {
        let index = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        &self.bases[index % self.bases.len()]
    }

    /// One exchange with endpoint hopping on transport failure only —
    /// the response (success or mapped error) is never re-targeted.
    pub(crate) async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, SdkError> {
        let mut last = SdkError::Transport("no Node attempted".to_owned());
        for _ in 0..self.bases.len() {
            let base = self.base().to_owned();
            match self.call_on(&base, method, path, body).await {
                Err(SdkError::Transport(error)) => last = SdkError::Transport(error),
                other => return other,
            }
        }
        Err(last)
    }

    /// One exchange against a named base — no rotation. Session-bound
    /// calls pin to the Node that opened them: rotating endpoints
    /// mid-session strands Node-local pacing/progress state (the
    /// ReaderPacingTable never sees a matching ack).
    pub(crate) async fn call_on(
        &self,
        base: &str,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, SdkError> {
        match self
            .transport
            .request(base, method, path, body, &self.api_key)
            .await
        {
            Ok(reply) if (200..300).contains(&reply.status) => Ok(reply.body),
            Ok(reply) => Err(map_error(reply.status, &reply.body)),
            Err(error) => Err(error),
        }
    }

    /// Binary-body append against a named base — no rotation.
    pub(crate) async fn call_on_binary_body(
        &self,
        base: &str,
        path: &str,
        body: &[u8],
    ) -> Result<Value, SdkError> {
        match self
            .transport
            .request_binary_body(base, path, body, &self.api_key)
            .await
        {
            Ok(reply) if (200..300).contains(&reply.status) => Ok(reply.body),
            Ok(reply) => Err(map_error(reply.status, &reply.body)),
            Err(error) => Err(error),
        }
    }

    /// Session-bound call pinned to `pinned` — hops to the next pooled
    /// base only on transport failure, then re-pins to the survivor.
    /// API refusals return unchanged (they are authoritative on every
    /// Node).
    pub(crate) async fn session_call(
        &self,
        pinned: &std::sync::Arc<std::sync::Mutex<String>>,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, SdkError> {
        let base = pinned.lock().expect("pinned base").clone();
        match self.call_on(&base, method, path, body).await {
            Err(SdkError::Transport(_)) => {
                let fallback = self.base().to_owned();
                let result = self.call_on(&fallback, method, path, body).await;
                if result.is_ok() {
                    *pinned.lock().expect("pinned base") = fallback;
                }
                result
            }
            other => other,
        }
    }

    /// Frame-page variant of [`Client::session_call`] — same
    /// pin-then-failover contract, but the reply is the decoded binary
    /// page plus its response headers (reader pacing advice rides
    /// `x-whitewater-*` headers on the binary wire).
    pub(crate) async fn session_call_frame_page(
        &self,
        pinned: &std::sync::Arc<std::sync::Mutex<String>>,
        path: &str,
        body: Option<&Value>,
    ) -> Result<TransportFramePageReply, SdkError> {
        let base = pinned.lock().expect("pinned base").clone();
        match self
            .transport
            .request_frame_page(&base, path, body, &self.api_key, None)
            .await
        {
            Err(SdkError::Transport(_)) => {
                let fallback = self.base().to_owned();
                let result = self
                    .transport
                    .request_frame_page(&fallback, path, body, &self.api_key, None)
                    .await;
                if result.is_ok() {
                    *pinned.lock().expect("pinned base") = fallback;
                }
                result
            }
            other => other,
        }
    }

    /// Meta-frame-page variant of [`Client::session_call`] for
    /// `members/fetch` — the `u32 meta_len` + JSON prelude + frame page
    /// wire, pinned like every session call.
    pub(crate) async fn session_call_meta_frame_page(
        &self,
        pinned: &std::sync::Arc<std::sync::Mutex<String>>,
        path: &str,
        body: Option<&Value>,
    ) -> Result<TransportMetaFramePageReply, SdkError> {
        let base = pinned.lock().expect("pinned base").clone();
        match self
            .transport
            .request_meta_frame_page(&base, path, body, &self.api_key)
            .await
        {
            Err(SdkError::Transport(_)) => {
                let fallback = self.base().to_owned();
                let result = self
                    .transport
                    .request_meta_frame_page(&fallback, path, body, &self.api_key)
                    .await;
                if result.is_ok() {
                    *pinned.lock().expect("pinned base") = fallback;
                }
                result
            }
            other => other,
        }
    }

    /// Binary-append variant of [`Client::session_call`] — same
    /// pin-then-failover contract for FSA1 frame bodies.
    pub(crate) async fn session_call_binary_body(
        &self,
        pinned: &std::sync::Arc<std::sync::Mutex<String>>,
        path: &str,
        body: &[u8],
    ) -> Result<Value, SdkError> {
        let base = pinned.lock().expect("pinned base").clone();
        match self.call_on_binary_body(&base, path, body).await {
            Err(SdkError::Transport(_)) => {
                let fallback = self.base().to_owned();
                let result = self.call_on_binary_body(&fallback, path, body).await;
                if result.is_ok() {
                    *pinned.lock().expect("pinned base") = fallback;
                }
                result
            }
            other => other,
        }
    }

    /// `call_binary` for frame pages: same base rotation and error
    /// mapping, decoded through the transport's streaming parser when
    /// available (whole-buffer fallback otherwise). `on_header` fires
    /// with the page's next_cursor as early as the transport can parse
    /// the header.
    pub(crate) async fn call_frame_page(
        &self,
        path: &str,
        body: Option<&Value>,
        mut on_header: Option<tokio::sync::oneshot::Sender<Option<String>>>,
    ) -> Result<(Vec<crate::model::Record>, Option<String>), SdkError> {
        let mut last = SdkError::Transport("no Node attempted".to_owned());
        for _ in 0..self.bases.len() {
            let base = self.base().to_owned();
            match self
                .transport
                .request_frame_page(&base, path, body, &self.api_key, on_header.take())
                .await
            {
                Ok(reply) if (200..300).contains(&reply.status) => {
                    return reply.page.ok_or_else(|| {
                        SdkError::Protocol("binary fetch page corrupt: missing page".to_owned())
                    })
                }
                Ok(reply) => {
                    let body: Value = serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
                    return Err(map_error(reply.status, &body));
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// Raw protocol escape hatch for ops without a typed façade — the
    /// same endpoint pool, bearer auth, and transport failure policy as
    /// every call, but the caller owns argument encoding and reads the
    /// raw `(status, body)` reply (including error statuses). The
    /// conformance runner drives shared fixtures through this surface.
    pub async fn wire(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<TransportReply, SdkError> {
        let mut last = SdkError::Transport("no Node attempted".to_owned());
        for _ in 0..self.bases.len() {
            let base = self.base().to_owned();
            match self
                .transport
                .request(&base, method, path, body, &self.api_key)
                .await
            {
                Ok(reply) => return Ok(reply),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// `GET /health` capability check: reports the server's protocol
    /// metadata; `check_compatible` turns a mismatched major into an
    /// explicit error instead of a silent capability gap.
    pub async fn health(&self) -> Result<Value, SdkError> {
        self.call("GET", "/health", None).await
    }

    pub async fn check_compatible(&self) -> Result<(), SdkError> {
        let health = self.health().await?;
        let reported = health
            .get("protocol")
            .and_then(|value| value.get("major"))
            .and_then(Value::as_u64)
            .or_else(|| {
                health
                    .get("version")
                    .and_then(Value::as_str)
                    .and_then(|v| v.split('.').next()?.parse::<u64>().ok())
            });
        match reported {
            Some(major) if major == SPEC_PROTOCOL_MAJOR => Ok(()),
            other => Err(SdkError::IncompatibleServer {
                expected: SPEC_PROTOCOL_MAJOR,
                reported: other.map(|major| major.to_string()).or_else(|| {
                    health
                        .get("version")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                }),
            }),
        }
    }

    /// The transport bounds this server enforces, decoded from
    /// `/health.limits`. A missing or malformed section is a protocol
    /// error — capability negotiation refuses silent downgrade.
    pub async fn transport_limits(&self) -> Result<TransportLimits, SdkError> {
        let health = self.health().await?;
        let limits = health
            .get("limits")
            .ok_or_else(|| SdkError::Protocol("health carries no limits section".to_owned()))?;
        let field = |name: &str| {
            limits
                .get(name)
                .and_then(Value::as_u64)
                .ok_or_else(|| SdkError::Protocol(format!("health limits missing {name}")))
        };
        Ok(TransportLimits {
            max_frame_bytes: field("max_frame_bytes")?,
            max_committed_read_bytes: field("max_committed_read_bytes")?,
            max_logical_read_frames: field("max_logical_read_frames")?,
            max_logical_read_ranges: field("max_logical_read_ranges")?,
            member_fetch_max_limit: field("subscription_member_fetch_max_limit")?,
            member_max_lease_ticks: field("subscription_member_max_lease_ticks")?,
        })
    }

    /// Execute a WCL script; the response is the per-statement
    /// `ControlExecution` result array.
    pub async fn wcl(&self, script: &str) -> Result<Vec<Value>, SdkError> {
        let body = self
            .call("POST", "/v1/admin/wcl", Some(&json!({ "script": script })))
            .await?;
        Ok(body
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| body.as_array().cloned().unwrap_or_default()))
    }

    /// Execute typed Control commands (the same verbs WCL lowers to).
    pub async fn commands(&self, commands: Vec<Value>) -> Result<Vec<Value>, SdkError> {
        let body = self
            .call(
                "POST",
                "/v1/admin/commands",
                Some(&json!({ "commands": commands })),
            )
            .await?;
        Ok(body
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    /// Run one declaration statement, tolerating an already-declared
    /// resource — the create-if-missing contract typed facades use.
    pub(crate) async fn create_resource_tolerating_exists(
        &self,
        script: &str,
    ) -> Result<(), SdkError> {
        match self.wcl(script).await {
            Ok(_) => Ok(()),
            Err(SdkError::Refused(detail)) | Err(SdkError::Conflict(detail))
                if detail.contains("already exists") =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Bind a declared Writer for appends: `OPEN WRITER SESSION` pins the
    /// session epoch the server fences every append against.
    pub(crate) async fn writer(&self, name: &str) -> Result<Writer, SdkError> {
        let base = self.base().to_owned();
        let results = self.wcl(&format!("OPEN WRITER SESSION {name};")).await?;
        let epoch = results
            .iter()
            .filter_map(|result| {
                result
                    .get("data")
                    .and_then(|data| data.get("session_epoch"))
                    .and_then(Value::as_u64)
                    .or_else(|| result.get("session_epoch").and_then(Value::as_u64))
            })
            .next()
            .ok_or_else(|| {
                SdkError::Protocol("OPEN WRITER SESSION returned no epoch".to_owned())
            })?;
        Ok(Writer {
            client: self.clone(),
            name: name.to_owned(),
            session_epoch: epoch,
            pinned: std::sync::Arc::new(std::sync::Mutex::new(base)),
            binary_appends: false,
        })
    }

    /// Bind a declared Reader: `reader.open` returns the session epoch
    /// and the current delivered/acknowledged cursors.
    pub(crate) async fn reader(&self, name: &str, capacity: u64) -> Result<Reader, SdkError> {
        let base = self.base().to_owned();
        let body = self
            .call(
                "POST",
                "/v1/readers/open",
                Some(&json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "reader": name,
                    "capacity": capacity,
                })),
            )
            .await?;
        let session_epoch = body
            .get("session_epoch")
            .and_then(Value::as_u64)
            .ok_or_else(|| SdkError::Protocol("reader.open returned no epoch".to_owned()))?;
        Ok(Reader {
            client: self.clone(),
            name: name.to_owned(),
            session_epoch,
            delivered_cursor: body
                .get("delivered_cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            acknowledged_cursor: body
                .get("acknowledged_cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            pinned: std::sync::Arc::new(std::sync::Mutex::new(base)),
        })
    }

    /// `CREATE SUBSCRIPTION <name> FROM <feed> [START AT <start>]` —
    /// the durable shared-progress lease `Member`s cooperate through.
    /// `start` defaults to [`ReaderStart::Beginning`]; `Now`,
    /// `Cursor`, and `TimestampNs` select other starts. Names pass
    /// through verbatim — the server enforces the dotted-name grammar.
    pub(crate) async fn create_subscription(
        &self,
        name: &str,
        feed: &str,
        start: Option<ReaderStart>,
    ) -> Result<Vec<Value>, SdkError> {
        let start = start.unwrap_or(ReaderStart::Beginning);
        self.wcl(&format!(
            "CREATE SUBSCRIPTION {name} FROM {feed} START AT {};",
            start.wcl()
        ))
        .await
    }

    /// Join a Subscription as a cooperating member.
    pub(crate) async fn member(&self, subscription: &str) -> Result<Member, SdkError> {
        let base = self.base().to_owned();
        let member_id = Uuid::new_v4();
        let body = self
            .call(
                "POST",
                "/v1/subscriptions/members/join",
                Some(&json!({
                    "subscription": subscription,
                    "request_id": member_id.to_string(),
                    "member_id": member_id.to_string(),
                })),
            )
            .await?;
        let member = Member::for_testing(
            self.clone(),
            subscription,
            member_id,
            body.get("member_epoch")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        );
        *member.pinned.lock().expect("pinned base") = base;
        Ok(member)
    }

    /// Read a page straight off a Feed: `after` resumes from an opaque
    /// Cursor, `limit` bounds the page. Requests the binary frame page
    /// first — a bulk history walk never pays the server-side base64 +
    /// JSON pass — and transparently reads the JSON shape when the
    /// serving Node or scripted transport does not speak frames.
    pub(crate) async fn records(
        &self,
        feed: &str,
        after: Option<&str>,
        limit: u64,
    ) -> Result<Vec<Record>, SdkError> {
        let mut path = format!("/v1/feeds/records?feed={feed}&limit={limit}");
        if let Some(cursor) = after {
            path.push_str(&format!("&after={}", urlencode(cursor)));
        }
        match self.records_frames(&path).await {
            Ok(records) => return Ok(records),
            Err(SdkError::Refused(_)) => {}
            Err(error) => return Err(error),
        }
        let body = self.call("GET", &path, None).await?;
        let records = body
            .as_array()
            .cloned()
            .or_else(|| body.get("records").and_then(Value::as_array).cloned())
            .unwrap_or_default();
        records.iter().map(decode_record).collect()
    }

    /// The binary arm of [`Client::records`]: rotates the endpoint pool
    /// like every stateless call and decodes either the frame page or
    /// the JSON body a frames-unaware Node answered with.
    async fn records_frames(&self, path: &str) -> Result<Vec<Record>, SdkError> {
        let mut last = SdkError::Transport("no Node attempted".to_owned());
        for _ in 0..self.bases.len() {
            let base = self.base().to_owned();
            match self
                .transport
                .request_frame_page_get(&base, path, &self.api_key)
                .await
            {
                Ok(reply) if (200..300).contains(&reply.status) => {
                    if let Some((records, _)) = reply.page {
                        return Ok(records);
                    }
                    let body: Value = serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
                    let records = body
                        .as_array()
                        .cloned()
                        .or_else(|| body.get("records").and_then(Value::as_array).cloned())
                        .unwrap_or_default();
                    return records.iter().map(decode_record).collect();
                }
                Ok(reply) => {
                    let body: Value = serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
                    return Err(map_error(reply.status, &body));
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// Bind a declared StateStore handle — no call; operations resolve
    /// the store per request and map its typed errors.
    pub(crate) fn state_store(&self, name: &str) -> StateStore {
        StateStore {
            client: self.clone(),
            name: name.to_owned(),
        }
    }

    /// Byte-level alias of `state_store` for call sites that read better
    /// next to the codec-bound [`Store`].
    pub(crate) fn state_store_raw(&self, name: &str) -> StateStore {
        self.state_store(name)
    }

    /// Codec-bound declared StateStore: UTF-8 strings both ways —
    /// plain JSON flows with no codec ceremony; [`store_with`] with
    /// [`Codec::bytes`] opts into raw binary.
    pub(crate) fn store(&self, name: &str) -> Store<String, String> {
        Store {
            inner: self.state_store(name),
            key_codec: Codec::utf8(),
            value_codec: Codec::utf8(),
        }
    }

    /// Codec-bound declared StateStore with explicit codecs.
    pub(crate) fn store_with<K, V>(
        &self,
        name: &str,
        key_codec: Codec<K>,
        value_codec: Codec<V>,
    ) -> Store<K, V> {
        Store {
            inner: self.state_store(name),
            key_codec,
            value_codec,
        }
    }

    /// Codec-bound declared Feed: UTF-8 strings both ways —
    /// un-schema-backed JSON flows with zero codec ceremony;
    /// [`feed_with`] with [`Codec::bytes`] is the escape hatch to raw
    /// bytes.
    pub(crate) fn feed(&self, name: &str) -> Feed<String, String> {
        Feed {
            client: self.clone(),
            name: name.to_owned(),
            key_codec: Codec::utf8(),
            value_codec: Codec::utf8(),
        }
    }

    /// Codec-bound declared Feed with explicit key/value codecs — JSON
    /// payloads use `Codec::json::<T>()`, custom formats use
    /// `Codec::custom`.
    pub(crate) fn feed_with<K, V>(
        &self,
        name: &str,
        key_codec: Codec<K>,
        value_codec: Codec<V>,
    ) -> Feed<K, V> {
        Feed {
            client: self.clone(),
            name: name.to_owned(),
            key_codec,
            value_codec,
        }
    }

    /// Bind a declared Pipe for lifecycle operations: `describe`
    /// reports the live catalog row while `pause`/`resume`/`drop`
    /// drive the same typed commands WCL lowers to.
    pub(crate) fn pipe(&self, name: &str) -> Pipe {
        Pipe::new(self.clone(), name.to_owned())
    }

    /// A lazy [`Domain`] handle — no I/O until a catalog accessor is
    /// awaited. `name` is validated locally against the dotted grammar.
    pub fn domain(&self, name: &str) -> Result<Domain, SdkError> {
        Domain::new(self.clone(), name)
    }

    /// An eager [`Domain`] handle — verifies the Domain exists and
    /// materializes its catalog snapshot now.
    pub async fn get_domain(&self, name: &str) -> Result<Domain, SdkError> {
        let domain = self.domain(name)?;
        domain.refresh().await?;
        Ok(domain)
    }

    /// A fluent [`ReaderBuilder`] bound to `feed` — `feed` must belong
    /// to `domain` (validated locally before any network call).
    ///
    /// ```ignore
    /// let reader = client
    ///     .create_reader(&shop, &shop.feed("orders")?)?
    ///     .reader_id("fulfillment")
    ///     .reader_start_point(ReaderStart::Beginning)
    ///     .open()
    ///     .await?;
    /// ```
    /// `set_domain_schema(domain, schema)` issues `SET SCHEMA ON
    /// DOMAIN` — the Domain's inherited default.
    pub async fn set_domain_schema(
        &self,
        domain: &str,
        schema: &FeedSchema,
    ) -> Result<Vec<serde_json::Value>, SdkError> {
        self.set_schema("domain", domain, schema).await
    }

    /// `set_feed_schema(feed, schema)` issues `SET SCHEMA ON FEED` —
    /// overrides the Domain's declared schema for the named sides.
    pub(crate) async fn set_feed_schema(
        &self,
        feed: &str,
        schema: &FeedSchema,
    ) -> Result<Vec<serde_json::Value>, SdkError> {
        self.set_schema("feed", feed, schema).await
    }

    /// `clear_domain_schema(domain)` issues `DROP SCHEMA ON DOMAIN`.
    pub async fn clear_domain_schema(
        &self,
        domain: &str,
    ) -> Result<Vec<serde_json::Value>, SdkError> {
        self.clear_schema("domain", domain).await
    }

    /// `clear_feed_schema(feed)` issues `DROP SCHEMA ON FEED` — the
    /// Feed reverts to inheriting its Domain's declared schema.
    pub(crate) async fn clear_feed_schema(
        &self,
        feed: &str,
    ) -> Result<Vec<serde_json::Value>, SdkError> {
        self.clear_schema("feed", feed).await
    }

    async fn set_schema(
        &self,
        kind: &str,
        name: &str,
        schema: &FeedSchema,
    ) -> Result<Vec<serde_json::Value>, SdkError> {
        self.commands(vec![serde_json::json!({
            "command": "set_schema",
            "scope": {"kind": kind, "name": name},
            "schema": schema,
        })])
        .await
    }

    async fn clear_schema(
        &self,
        kind: &str,
        name: &str,
    ) -> Result<Vec<serde_json::Value>, SdkError> {
        self.commands(vec![serde_json::json!({
            "command": "clear_schema",
            "scope": {"kind": kind, "name": name},
        })])
        .await
    }

    pub(crate) fn create_reader<K, V>(
        &self,
        domain: &Domain,
        feed: &Feed<K, V>,
    ) -> Result<ReaderBuilder<K, V>, SdkError> {
        if !domain.owns(feed.name()) {
            return Err(SdkError::Refused(format!(
                "feed '{}' does not belong to domain '{}'",
                feed.name(),
                domain.name()
            )));
        }
        Ok(ReaderBuilder::new(self.clone(), feed.clone()))
    }

    /// A fluent [`WriterBuilder`] bound to `feed` — `feed` must belong
    /// to `domain` (validated locally before any network call).
    pub(crate) fn create_writer<K, V>(
        &self,
        domain: &Domain,
        feed: &Feed<K, V>,
    ) -> Result<WriterBuilder<K, V>, SdkError> {
        if !domain.owns(feed.name()) {
            return Err(SdkError::Refused(format!(
                "feed '{}' does not belong to domain '{}'",
                feed.name(),
                domain.name()
            )));
        }
        Ok(WriterBuilder::new(self.clone(), feed.clone()))
    }

    /// `DEFINE PIPE` for a forward (copy) Pipe: each record the
    /// Subscription delivers lands unchanged on `output_feed`. Names
    /// pass through verbatim — the server enforces the dotted-name
    /// grammar and returns the catalog row as a [`PipeInfo`]-shaped
    /// result entry.
    pub(crate) async fn define_forward_pipe(
        &self,
        name: &str,
        subscription: &str,
        output_feed: &str,
    ) -> Result<Vec<Value>, SdkError> {
        self.commands(vec![json!({
            "command": "define_pipe",
            "name": name,
            "subscription": subscription,
            "output_feed": output_feed,
            "operation": PipeOperation::forward().to_wire(),
        })])
        .await
    }

    /// `DEFINE PIPE` for an enrich Pipe: each record the Subscription
    /// delivers resolves one `output_store` row through the operation's
    /// `key` and lands on `output_feed` with the journaled lookup
    /// result. `operation` must come from [`PipeOperation::enrich`] —
    /// a mismatched step refuses locally. `quarantine_feed` names the
    /// miss-quarantine target and is valid only when the operation's
    /// `on_miss` is [`EnrichMiss::Quarantine`]; that combination is
    /// also required — a quarantine miss without a feed refuses locally.
    pub(crate) async fn define_enrich_pipe(
        &self,
        name: &str,
        subscription: &str,
        output_feed: &str,
        output_store: &str,
        operation: impl Into<PipeOperation>,
        quarantine_feed: Option<&str>,
    ) -> Result<Vec<Value>, SdkError> {
        let operation = operation.into();
        let PipeOperation::Enrich { key, on_miss, .. } = &operation else {
            return Err(SdkError::Refused(
                "enrich pipes take PipeOperation::enrich(...)".to_owned(),
            ));
        };
        key.validate()?;
        match (on_miss, quarantine_feed) {
            (EnrichMiss::Quarantine, None) => {
                return Err(SdkError::Refused(
                    "on_miss quarantine requires a quarantine_feed".to_owned(),
                ));
            }
            (_, Some(_)) if *on_miss != EnrichMiss::Quarantine => {
                return Err(SdkError::Refused(
                    "a quarantine_feed is only valid with on_miss quarantine".to_owned(),
                ));
            }
            _ => {}
        }
        let mut command = json!({
            "command": "define_pipe",
            "name": name,
            "subscription": subscription,
            "output_feed": output_feed,
            "output_store": output_store,
            "operation": operation.to_wire(),
        });
        if let Some(feed) = quarantine_feed {
            command["quarantine_feed"] = json!(feed);
        }
        self.commands(vec![command]).await
    }

    /// `DEFINE PIPE` for a window Pipe: each record the Subscription
    /// delivers accumulates into `window_ns`-wide panes of the
    /// `output_store` view (which must declare non-unique named keys
    /// `key` and `pane`). `operation` must come from
    /// [`PipeOperation::window`] — a mismatched step refuses locally,
    /// as do the lateness/retention bound combinations the server
    /// rejects. `emit_feed` publishes closed panes and requires the
    /// operation to declare `max_late_ns` or `watermark_ns` — without a
    /// lateness bound no pane ever provably closes.
    pub(crate) async fn define_window_pipe(
        &self,
        name: &str,
        subscription: &str,
        output_store: &str,
        operation: impl Into<PipeOperation>,
        emit_feed: Option<&str>,
    ) -> Result<Vec<Value>, SdkError> {
        let operation = operation.into();
        let PipeOperation::Window(window) = &operation else {
            return Err(SdkError::Refused(
                "window pipes take PipeOperation::window(...)".to_owned(),
            ));
        };
        window.validate(emit_feed.is_some())?;
        let mut command = json!({
            "command": "define_pipe",
            "name": name,
            "subscription": subscription,
            "output_store": output_store,
            "operation": operation.to_wire(),
        });
        if let Some(feed) = emit_feed {
            command["emit_feed"] = json!(feed);
        }
        self.commands(vec![command]).await
    }

    /// `DEFINE STATESTORE`: declare `spec` on the Control Plane and
    /// return it bound to this client. `source` names a same-Domain Feed
    /// the store materializes from; `None` means a manual store.
    /// Feed-sourced named keys must each declare an `extractor` so
    /// replay can project typed values; extractors on a manual store
    /// are refused.
    pub(crate) async fn define_state_store(
        &self,
        spec: StateStoreSpec,
        source: Option<&str>,
    ) -> Result<StateStore, SdkError> {
        if source.is_none() && spec.named_keys().iter().any(|key| key.extractor.is_some()) {
            return Err(SdkError::Refused(
                "named-key extractors apply to feed-sourced StateStores; \
                 manual stores take named values on each put_row mutation"
                    .to_owned(),
            ));
        }
        if source.is_some() && spec.named_keys().iter().any(|key| key.extractor.is_none()) {
            return Err(SdkError::Refused(
                "feed-sourced named keys require an extractor \
                 (Extractor::record_key()/metadata()/json())"
                    .to_owned(),
            ));
        }
        for key in spec.named_keys() {
            if let Some(extractor) = &key.extractor {
                extractor.validate()?;
            }
        }
        self.commands(vec![json!({
            "command": "define_state_store",
            "name": spec.name(),
            "source": match source {
                Some(feed) => json!({ "kind": "feed", "feed": feed }),
                None => json!({ "kind": "manual" }),
            },
            "named_keys": spec
                .named_keys()
                .iter()
                .map(|key| {
                    let mut entry = json!({ "name": key.name, "unique": key.unique });
                    if let Some(extractor) = &key.extractor {
                        entry["extractor"] = extractor.to_wire();
                    }
                    entry
                })
                .collect::<Vec<_>>(),
        })])
        .await?;
        Ok(self.state_store(spec.name()))
    }

    /// `CREATE DOMAIN <name>` and return the lazy [`Domain`] handle —
    /// the only front door for Feeds, Writers, Readers, Subscriptions,
    /// Pipes, StateStores, and Indexes. Use [`Client::domain`] for a
    /// handle without declaring, [`Client::get_domain`] to verify and
    /// materialize the catalog.
    pub async fn create_domain(&self, name: &str) -> Result<Domain, SdkError> {
        let domain = Domain::new(self.clone(), name)?;
        self.wcl(&format!("CREATE DOMAIN {}", domain.name()))
            .await?;
        Ok(domain)
    }

    /// `SHOW PIPES` decoded as [`PipeInfo`] rows.
    pub(crate) async fn pipes(&self) -> Result<Vec<PipeInfo>, SdkError> {
        let results = self
            .commands(vec![json!({ "command": "show", "kind": "pipes" })])
            .await?;
        let rows = results
            .first()
            .and_then(|result| result.get("data"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(decode_pipes(&rows))
    }

    /// Temporary read against a Feed without declaring a Reader:
    /// bounded history walks, live tails, or fresh-only waits per
    /// `options` — see `TemporaryFetch`.
    pub(crate) async fn temporary_fetch(
        &self,
        feed: &str,
        options: TemporaryFetch,
    ) -> Result<TemporaryPage, SdkError> {
        let body = self
            .call(
                "POST",
                "/v1/readers/temporary/fetch",
                Some(&json!({
                    "feed": feed,
                    "after": options.after,
                    "limit": options.limit,
                    "new_only": options.new_only,
                    "tail": options.tail,
                    "wait_ms": options.wait_ms,
                    "after_event_time_ns": options
                        .after_event_time_ns
                        .map(|nanos| nanos.to_string()),
                })),
            )
            .await?;
        let records = body
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TemporaryPage {
            records,
            next_cursor: body
                .get("next_cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    }

    /// [`Client::temporary_fetch`] on the binary wire: identical
    /// request and page semantics, but the reply streams stored record
    /// frames (`application/vnd.whitewater.frames`) and the SDK decodes
    /// them — a bulk replay never pays server-side base64 + JSON
    /// encode/decode. The records are the same [`Record`]s a JSON page
    /// returns.
    pub(crate) async fn temporary_fetch_frames(
        &self,
        feed: &str,
        options: TemporaryFetch,
    ) -> Result<TemporaryPage, SdkError> {
        let (records, next_cursor) = self
            .call_frame_page(
                "/v1/readers/temporary/fetch",
                Some(&json!({
                    "feed": feed,
                    "after": options.after,
                    "limit": options.limit,
                    "new_only": options.new_only,
                    "tail": options.tail,
                    "wait_ms": options.wait_ms,
                    "after_event_time_ns": options
                        .after_event_time_ns
                        .map(|nanos| nanos.to_string()),
                })),
                None,
            )
            .await?;
        Ok(TemporaryPage {
            records,
            next_cursor,
        })
    }

    /// Pipelined variant of [`Client::temporary_fetch_frames`]: an
    /// async iterator that issues the request for page k+1 — whose
    /// `after` cursor is already known from page k's header — while the
    /// caller drains page k. `depth` bounds in-flight prefetching
    /// (page cursors chain serially, so >1 has no additional pages to
    /// launch; 0 behaves like the sequential call). Pages yield in
    /// order; an error or an empty page (with no `wait_ms`) ends the
    /// stream.
    pub(crate) fn temporary_fetch_frames_pipelined(
        &self,
        feed: &str,
        options: TemporaryFetch,
        depth: usize,
    ) -> TemporaryFramePageStream {
        TemporaryFramePageStream {
            shared: std::sync::Arc::new(FramePageShared {
                client: self.clone(),
                feed: feed.to_owned(),
                template: options,
                depth: depth.max(1),
                pending: std::sync::Mutex::new(std::collections::VecDeque::new()),
                open: std::sync::Mutex::new(0),
                done: std::sync::atomic::AtomicBool::new(false),
            }),
            done: false,
        }
    }

    /// Query a declared persisted Index. `mode` selects exact,
    /// prefix, or range matching per `sdk/spec/endpoints.json`; `strict`
    /// bounded-waits for the serving Node's checkpoint to reach the
    /// activation frontier, surfacing `IndexBehind` when it cannot.
    pub(crate) async fn query_index(
        &self,
        index: &str,
        mode: IndexQuery,
        limit: Option<u64>,
        strict: bool,
    ) -> Result<IndexPage, SdkError> {
        let mode_fields = match &mode {
            IndexQuery::Exact(values) => json!({
                "mode": "exact",
                "values": values.iter().map(wire_typed).collect::<Vec<_>>(),
            }),
            IndexQuery::Prefix {
                leading,
                partial_text,
            } => json!({
                "mode": "prefix",
                "leading": leading.iter().map(wire_typed).collect::<Vec<_>>(),
                "partial_text": partial_text,
            }),
            IndexQuery::Range {
                leading,
                lower,
                upper,
            } => json!({
                "mode": "range",
                "leading": leading.iter().map(wire_typed).collect::<Vec<_>>(),
                "lower": {"value": wire_typed(&lower.value), "inclusive": lower.inclusive},
                "upper": {"value": wire_typed(&upper.value), "inclusive": upper.inclusive},
            }),
        };
        let mut request = json!({
            "index": index,
            "strict": strict,
        });
        if let Some(object) = request.as_object_mut() {
            if let Some(fields) = mode_fields.as_object() {
                for (key, value) in fields {
                    object.insert(key.clone(), value.clone());
                }
            }
            if let Some(limit) = limit {
                object.insert("limit".to_owned(), json!(limit));
            }
        }
        let body = self
            .call("POST", "/v1/indexes/query", Some(&request))
            .await?;
        decode_index_page(&body)
    }
}

/// The page stream returned by [`Client::temporary_fetch_frames_pipelined`]:
/// poll [`next`](Self::next) like a manual `Stream`.
pub struct TemporaryFramePageStream {
    shared: std::sync::Arc<FramePageShared>,
    done: bool,
}

type SpawnedPage =
    tokio::task::JoinHandle<Result<(Vec<crate::model::Record>, Option<String>), SdkError>>;

struct FramePageShared {
    client: Client,
    feed: String,
    template: TemporaryFetch,
    depth: usize,
    pending: std::sync::Mutex<std::collections::VecDeque<SpawnedPage>>,
    open: std::sync::Mutex<usize>,
    done: std::sync::atomic::AtomicBool,
}

impl FramePageShared {
    fn spawn_page(self: &std::sync::Arc<Self>, after: Option<String>) {
        if self.done.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let mut open = self.open.lock().expect("prefetch slot");
        if *open >= self.depth {
            return;
        }
        *open += 1;
        drop(open);
        let shared = self.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut options = self.template.clone();
        options.after = after;
        let client = self.client.clone();
        let feed = self.feed.clone();
        let handle = tokio::spawn(async move {
            let body = json!({
                "feed": feed,
                "after": options.after,
                "limit": options.limit,
                "new_only": options.new_only,
                "tail": options.tail,
                "wait_ms": options.wait_ms,
                "after_event_time_ns": options
                    .after_event_time_ns
                    .map(|nanos| nanos.to_string()),
            });
            client
                .call_frame_page("/v1/readers/temporary/fetch", Some(&body), Some(tx))
                .await
        });
        self.pending
            .lock()
            .expect("pending pages")
            .push_back(handle);
        // The page's next_cursor arrives while its body still streams —
        // chain the next request immediately.
        let shared = shared;
        tokio::spawn(async move {
            if let Ok(Some(cursor)) = rx.await {
                shared.spawn_page(Some(cursor));
            }
        });
    }

    fn release_slot(&self) {
        let mut open = self.open.lock().expect("prefetch slot");
        *open = open.saturating_sub(1);
    }
}

impl TemporaryFramePageStream {
    /// The next page in order, or `None` at the tip/after an error.
    pub async fn next(&mut self) -> Option<Result<TemporaryPage, SdkError>> {
        if self.done {
            return None;
        }
        {
            let pending = self.shared.pending.lock().expect("pending pages");
            if pending.is_empty() {
                drop(pending);
                self.shared.spawn_page(self.shared.template.after.clone());
            }
        }
        let handle = {
            let mut pending = self.shared.pending.lock().expect("pending pages");
            pending.pop_front()
        };
        let Some(handle) = handle else {
            self.done = true;
            return None;
        };
        let (records, next_cursor) = match handle.await {
            Ok(Ok(page)) => page,
            Ok(Err(error)) => {
                self.done = true;
                self.shared
                    .done
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return Some(Err(error));
            }
            Err(join) => {
                self.done = true;
                self.shared
                    .done
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return Some(Err(SdkError::Transport(format!("page task: {join}"))));
            }
        };
        self.shared.release_slot();
        if records.is_empty() && self.shared.template.wait_ms.is_none() {
            self.done = true;
            self.shared
                .done
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if next_cursor.is_none() {
            self.done = true;
            self.shared
                .done
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Some(Ok(TemporaryPage {
            records,
            next_cursor,
        }))
    }
}

impl Drop for TemporaryFramePageStream {
    fn drop(&mut self) {
        self.shared
            .done
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut pending = self.shared.pending.lock().expect("pending pages");
        for handle in pending.drain(..) {
            handle.abort();
        }
    }
}
