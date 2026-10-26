//! The bound endpoint pool: one `Client` spreads calls across any set
//! of Nodes — owner routing is a server detail that never crosses this
//! boundary.

use std::sync::Arc;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::codec::Codec;
use crate::domain::{Domain, FeedSchema, ReaderBuilder, WriterBuilder};
use crate::error::{map_error, SdkError};
use crate::feed::Feed;
use crate::member::Member;
use crate::model::{
    decode_record, urlencode, Record, TemporaryFetch, TemporaryPage, TransportLimits,
};
use crate::pipe::{decode_pipes, Pipe, PipeInfo};
use crate::reader::Reader;
use crate::store::StateStore;
use crate::transport::{HttpTransport, Transport, TransportReply};
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
            match self
                .transport
                .request(&base, method, path, body, &self.api_key)
                .await
            {
                Ok(reply) if (200..300).contains(&reply.status) => return Ok(reply.body),
                Ok(reply) => return Err(map_error(reply.status, &reply.body)),
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
    pub async fn writer(&self, name: &str) -> Result<Writer, SdkError> {
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
        })
    }

    /// Bind a declared Reader: `reader.open` returns the session epoch
    /// and the current delivered/acknowledged cursors.
    pub async fn reader(&self, name: &str, capacity: u64) -> Result<Reader, SdkError> {
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
        })
    }

    /// Join a Subscription as a cooperating member.
    pub async fn member(&self, subscription: &str) -> Result<Member, SdkError> {
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
        Ok(Member::for_testing(
            self.clone(),
            subscription,
            member_id,
            body.get("member_epoch")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        ))
    }

    /// Read a page straight off a Feed: `after` resumes from an opaque
    /// Cursor, `limit` bounds the page.
    pub async fn records(
        &self,
        feed: &str,
        after: Option<&str>,
        limit: u64,
    ) -> Result<Vec<Record>, SdkError> {
        let mut path = format!("/v1/feeds/records?feed={feed}&limit={limit}");
        if let Some(cursor) = after {
            path.push_str(&format!("&after={}", urlencode(cursor)));
        }
        let body = self.call("GET", &path, None).await?;
        let records = body
            .as_array()
            .cloned()
            .or_else(|| body.get("records").and_then(Value::as_array).cloned())
            .unwrap_or_default();
        records.iter().map(decode_record).collect()
    }

    /// Bind a declared StateStore handle — no call; operations resolve
    /// the store per request and map its typed errors.
    pub fn state_store(&self, name: &str) -> StateStore {
        StateStore {
            client: self.clone(),
            name: name.to_owned(),
        }
    }

    /// Byte-level alias of `state_store` for call sites that read better
    /// next to the codec-bound [`Store`].
    pub fn state_store_raw(&self, name: &str) -> StateStore {
        self.state_store(name)
    }

    /// Codec-bound declared StateStore: utf8 keys, raw byte payloads —
    /// the cross-SDK default binding.
    pub fn store(&self, name: &str) -> Store<String, Vec<u8>> {
        Store {
            inner: self.state_store(name),
            key_codec: Codec::utf8(),
            value_codec: Codec::bytes(),
        }
    }

    /// Codec-bound declared StateStore with explicit codecs.
    pub fn store_with<K, V>(
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

    /// Codec-bound declared Feed: utf8 keys, raw byte payloads — the
    /// cross-SDK default binding.
    pub fn feed(&self, name: &str) -> Feed<String, Vec<u8>> {
        Feed {
            client: self.clone(),
            name: name.to_owned(),
            key_codec: Codec::utf8(),
            value_codec: Codec::bytes(),
        }
    }

    /// Codec-bound declared Feed with explicit key/value codecs — JSON
    /// payloads use `Codec::json::<T>()`, custom formats use
    /// `Codec::custom`.
    pub fn feed_with<K, V>(
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
    pub fn pipe(&self, name: &str) -> Pipe {
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
    pub async fn set_feed_schema(
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
    pub async fn clear_feed_schema(&self, feed: &str) -> Result<Vec<serde_json::Value>, SdkError> {
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

    pub fn create_reader<K, V>(
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

    /// [`Client::create_reader`] taking a feed name relative to `domain`
    /// (`"orders"` resolves to `shop.orders`) with UTF-8 keys and byte
    /// payloads.
    pub fn create_reader_named(
        &self,
        domain: &Domain,
        feed: &str,
    ) -> Result<ReaderBuilder<String, Vec<u8>>, SdkError> {
        self.create_reader(domain, &domain.feed(feed)?)
    }

    /// A fluent [`WriterBuilder`] bound to `feed` — `feed` must belong
    /// to `domain` (validated locally before any network call).
    pub fn create_writer<K, V>(
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

    /// [`Client::create_writer`] taking a feed name relative to
    /// `domain` with UTF-8 keys and byte payloads.
    pub fn create_writer_named(
        &self,
        domain: &Domain,
        feed: &str,
    ) -> Result<WriterBuilder<String, Vec<u8>>, SdkError> {
        self.create_writer(domain, &domain.feed(feed)?)
    }

    /// `SHOW PIPES` decoded as [`PipeInfo`] rows.
    pub async fn pipes(&self) -> Result<Vec<PipeInfo>, SdkError> {
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
    pub async fn temporary_fetch(
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

    /// Query a declared persisted Index. `mode` selects exact,
    /// prefix, or range matching per `sdk/spec/endpoints.json`; `strict`
    /// bounded-waits for the serving Node's checkpoint to reach the
    /// activation frontier, surfacing `IndexBehind` when it cannot.
    pub async fn query_index(
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
