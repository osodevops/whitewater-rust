//! Strongly-typed Domain façade — the administrative namespace that owns
//! every Feed whose name it prefixes (`shop` owns `shop.orders`):
//!
//! ```ignore
//! let shop = client.get_domain("shop").await?;       // eager — verifies + materializes
//! let shop = client.domain("shop")?;                  // lazy — no I/O until an accessor
//!
//! let mut reader = client
//!     .create_reader(&shop, &shop.feed("orders"))?
//!     .reader_id("fulfillment")                       // the Kafka groupId analogue
//!     .reader_start_point(ReaderStart::Beginning)     // first-time start only
//!     .max_page_size(256)
//!     .open()
//!     .await?;
//! ```
//!
//! The first catalog accessor materializes the Domain's catalog view in
//! one batched control read — `DESCRIBE DOMAIN` plus every SHOW listing —
//! and caches it. [`Domain::refresh`] re-pulls. [`Domain::feed`] is pure
//! name composition and never performs I/O, so
//! `client.create_reader(&domain, &domain.feed("orders"))` costs no extra
//! round trips.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use crate::client::Client;
use crate::codec::Codec;
use crate::consumer::ReaderStart;
use crate::error::SdkError;
use crate::feed::{Feed, FeedReader, FeedWriter};

/// Default max page size for [`ReaderBuilder::open`].
const DEFAULT_READER_CAPACITY: u64 = 128;

/// One side of a Feed's schema metadata — format plus optional
/// subject/definition text. Formats: `json`, `jsonschema`, `avro`,
/// `protobuf`, `text`, `bytes`, or `custom.<name>`. Records stay schema-neutral bytes on the
/// wire; declarations are catalog metadata for tooling and
/// compatibility — not yet an enforced registry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaDecl {
    /// The declared format (`json`, `avro`, `custom.<name>`, …).
    pub format: String,
    /// Optional subject/version identity for tooling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Optional inline definition text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
}

/// The declared key/value schema pair — either side may be absent. A
/// Feed's declaration overrides the named sides of its Domain default.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FeedSchema {
    /// The record-key schema, when declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<SchemaDecl>,
    /// The record-value schema, when declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<SchemaDecl>,
}

impl FeedSchema {
    fn from_wire(row: Option<&Value>) -> Option<Self> {
        let parsed: FeedSchema = serde_json::from_value(row?.clone()).unwrap_or_default();
        if parsed.key.is_none() && parsed.value.is_none() {
            return None;
        }
        Some(parsed)
    }
}

/// A strongly-typed handle on one Domain. Construct via
/// [`Client::domain`] (lazy) or [`Client::get_domain`] (eager — verifies
/// the Domain exists and loads the snapshot immediately).
pub struct Domain {
    client: Client,
    name: String,
    snapshot: Mutex<Option<Arc<Snapshot>>>,
}

/// One catalog view of a Domain — the describe row plus every scoped
/// resource listing, joined on `space_id`/`feed_id`.
struct Snapshot {
    domain_id: String,
    describe: Value,
    feeds: Vec<String>,
    feed_schemas: std::collections::HashMap<String, Option<FeedSchema>>,
    declared_schemas: std::collections::HashMap<String, Option<FeedSchema>>,
    readers: Vec<String>,
    writers: Vec<String>,
    subscriptions: Vec<String>,
    pipes: Vec<String>,
    indexes: Vec<String>,
    state_stores: Vec<String>,
}

impl Domain {
    pub(crate) fn new(client: Client, name: &str) -> Result<Self, SdkError> {
        Ok(Self {
            client,
            name: dotted(name)?,
            snapshot: Mutex::new(None),
        })
    }

    /// The Domain's dotted name (`"shop"`).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The Domain's immutable id — survives renames. Materializes the
    /// snapshot on first call.
    pub async fn domain_id(&self) -> Result<String, SdkError> {
        Ok(self.snap().await?.domain_id.clone())
    }

    /// The raw `DESCRIBE DOMAIN` row — status, `created_at_ns`,
    /// `history_policy`, and friends.
    pub async fn describe(&self) -> Result<Value, SdkError> {
        Ok(self.snap().await?.describe.clone())
    }

    /// The Domain's declared schema default — `None` when undeclared.
    /// Every Feed without its own declaration inherits it.
    pub async fn schema(&self) -> Result<Option<FeedSchema>, SdkError> {
        Ok(FeedSchema::from_wire(
            self.snap().await?.describe.get("schema"),
        ))
    }

    /// The effective schema for a Feed — its declared sides layered
    /// over the Domain default. `None` when nothing is declared
    /// anywhere (schema-neutral bytes).
    pub async fn feed_schema(&self, feed: &str) -> Result<Option<FeedSchema>, SdkError> {
        Ok(self
            .snap()
            .await?
            .feed_schemas
            .get(feed)
            .cloned()
            .unwrap_or(None))
    }

    /// Only the sides the Feed itself declares — `None` when it
    /// inherits the Domain default wholesale.
    pub async fn declared_feed_schema(&self, feed: &str) -> Result<Option<FeedSchema>, SdkError> {
        Ok(self
            .snap()
            .await?
            .declared_schemas
            .get(feed)
            .cloned()
            .unwrap_or(None))
    }

    /// Every Feed in this Domain as typed [`Feed`] handles bound to
    /// UTF-8 keys and byte payloads, sorted by name. Materializes the
    /// snapshot.
    pub async fn feeds(&self) -> Result<Vec<Feed<String, Vec<u8>>>, SdkError> {
        self.feeds_prefixed(None).await
    }

    /// [`Domain::feeds`] filtered to relative names starting with
    /// `prefix` (`Some("order")` matches `shop.orders` and
    /// `shop.order.archive`).
    pub async fn feeds_prefixed(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<Feed<String, Vec<u8>>>, SdkError> {
        let snap = self.snap().await?;
        Ok(snap
            .feeds
            .iter()
            .filter(|full| prefix.is_none_or(|p| relative(&self.name, full).starts_with(p)))
            .map(|full| self.client.feed_with(full, Codec::utf8(), Codec::bytes()))
            .collect())
    }

    /// The qualified Feed names in this Domain, sorted — the
    /// plain-string view of [`Domain::feeds`].
    pub async fn feed_names(&self) -> Result<Vec<String>, SdkError> {
        Ok(self.snap().await?.feeds.clone())
    }

    /// Names of declared Readers on this Domain's Feeds, sorted.
    pub async fn readers(&self) -> Result<Vec<String>, SdkError> {
        Ok(self.snap().await?.readers.clone())
    }

    /// Names of declared Writers on this Domain's Feeds, sorted.
    pub async fn writers(&self) -> Result<Vec<String>, SdkError> {
        Ok(self.snap().await?.writers.clone())
    }

    /// Names of declared Subscriptions in this Domain, sorted.
    pub async fn subscriptions(&self) -> Result<Vec<String>, SdkError> {
        Ok(self.snap().await?.subscriptions.clone())
    }

    /// Names of declared Pipes in this Domain, sorted.
    pub async fn pipes(&self) -> Result<Vec<String>, SdkError> {
        Ok(self.snap().await?.pipes.clone())
    }

    /// Names of declared Indexes in this Domain, sorted.
    pub async fn indexes(&self) -> Result<Vec<String>, SdkError> {
        Ok(self.snap().await?.indexes.clone())
    }

    /// Names of declared StateStores in this Domain, sorted.
    pub async fn state_stores(&self) -> Result<Vec<String>, SdkError> {
        Ok(self.snap().await?.state_stores.clone())
    }

    /// Re-pull the catalog snapshot — the next accessor sees fresh rows.
    pub async fn refresh(&self) -> Result<(), SdkError> {
        let loaded = Arc::new(self.load().await?);
        *self.snapshot.lock().unwrap() = Some(loaded);
        Ok(())
    }

    // ---- Feed handles ---------------------------------------------------------

    /// A typed [`Feed`] handle on `relative` inside this Domain:
    /// `shop.feed("orders")` binds `shop.orders`. An already-qualified
    /// name (`"shop.orders"`) is accepted; every result lives under the
    /// Domain's prefix by construction. Pure name composition — no I/O.
    pub fn feed(&self, relative: &str) -> Result<Feed<String, Vec<u8>>, SdkError> {
        self.feed_with(relative, Codec::utf8(), Codec::bytes())
    }

    /// [`Domain::feed`] with explicit key/value codecs — JSON payloads
    /// use [`Codec::json`], custom formats use [`Codec::custom`].
    pub fn feed_with<K, V>(
        &self,
        relative: &str,
        key_codec: Codec<K>,
        value_codec: Codec<V>,
    ) -> Result<Feed<K, V>, SdkError> {
        Ok(self
            .client
            .feed_with(&self.resolve(relative)?, key_codec, value_codec))
    }

    /// [`Domain::feed`] after verifying the Feed is declared in the
    /// catalog. Materializes the snapshot.
    pub async fn get_feed(&self, relative: &str) -> Result<Feed<String, Vec<u8>>, SdkError> {
        let full = self.resolve(relative)?;
        if !self.snap().await?.feeds.iter().any(|feed| feed == &full) {
            return Err(SdkError::Refused(format!(
                "feed '{full}' is not declared in domain '{}'",
                self.name
            )));
        }
        self.feed(relative)
    }

    /// `CREATE FEED` inside this Domain — `shop.create_feed("orders")`
    /// declares `shop.orders`. [`Domain::refresh`] (or a lazy
    /// accessor's first load) surfaces it through
    /// [`Domain::feeds`]/[`Domain::feed_names`].
    pub async fn create_feed(&self, relative: &str) -> Result<Vec<Value>, SdkError> {
        let full = self.resolve(relative)?;
        self.client.wcl(&format!("CREATE FEED {full}")).await
    }

    /// Fluent Reader declaration on `relative` inside this Domain —
    /// `shop.create_reader("orders")` binds `shop.orders`:
    ///
    /// ```ignore
    /// let mut reader = shop
    ///     .create_reader("orders")?
    ///     .reader_id("fulfillment")               // the Kafka groupId analogue
    ///     .reader_start_point(ReaderStart::Beginning)
    ///     .open()
    ///     .await?;
    /// ```
    ///
    /// [`Client::create_reader`] accepts the typed [`Feed`] handle.
    pub fn create_reader(
        &self,
        relative: &str,
    ) -> Result<ReaderBuilder<String, Vec<u8>>, SdkError> {
        self.client.create_reader(self, &self.feed(relative)?)
    }

    /// Fluent Writer declaration on `relative` inside this Domain —
    /// `shop.create_writer("orders")` binds `shop.orders`.
    /// [`Client::create_writer`] accepts the typed [`Feed`] handle.
    pub fn create_writer(
        &self,
        relative: &str,
    ) -> Result<WriterBuilder<String, Vec<u8>>, SdkError> {
        self.client.create_writer(self, &self.feed(relative)?)
    }

    /// Resolve `relative` to a full dotted Feed name inside this Domain
    /// — accepts the short form (`"orders"`) or the already-qualified
    /// form (`"shop.orders"`). A name that fails the dotted grammar is
    /// refused here, locally.
    pub(crate) fn resolve(&self, relative: &str) -> Result<String, SdkError> {
        let full = if relative.starts_with(&format!("{}.", self.name)) {
            relative.to_owned()
        } else {
            format!("{}.{}", self.name, relative)
        };
        dotted(&full)
    }

    /// `true` when `feed` lives under this Domain's name prefix —
    /// cross-domain Reader/Writer construction refuses locally.
    pub(crate) fn owns(&self, feed_name: &str) -> bool {
        feed_name.starts_with(&format!("{}.", self.name))
    }

    // ---- snapshot loading ------------------------------------------------------

    async fn snap(&self) -> Result<Arc<Snapshot>, SdkError> {
        if let Some(snap) = self.snapshot.lock().unwrap().as_ref() {
            return Ok(snap.clone());
        }
        let loaded = Arc::new(self.load().await?);
        let mut guard = self.snapshot.lock().unwrap();
        Ok(guard.get_or_insert(loaded).clone())
    }

    /// One batched control read: `DESCRIBE DOMAIN` plus every SHOW
    /// listing. Rows join on `space_id`; readers and writers join
    /// through their Feed's `feed_id`. A missing Domain fails the whole
    /// batch server-side and surfaces as [`SdkError::Refused`].
    async fn load(&self) -> Result<Snapshot, SdkError> {
        let results = self
            .client
            .commands(vec![
                json!({ "command": "describe", "kind": "domain", "name": self.name }),
                json!({ "command": "show", "kind": "feeds" }),
                json!({ "command": "show", "kind": "readers" }),
                json!({ "command": "show", "kind": "writers" }),
                json!({ "command": "show", "kind": "subscriptions" }),
                json!({ "command": "show", "kind": "pipes" }),
                json!({ "command": "show", "kind": "indexes" }),
                json!({ "command": "show", "kind": "state_stores" }),
            ])
            .await?;

        let describe = results
            .first()
            .and_then(|result| result.get("data"))
            .cloned()
            .unwrap_or(Value::Null);
        let domain_id = describe
            .get("space_id")
            .and_then(Value::as_str)
            .ok_or_else(|| SdkError::Refused(format!("domain '{}' is not declared", self.name)))?
            .to_owned();

        let mut feeds = Vec::new();
        let mut feed_ids = Vec::new();
        let mut feed_schemas = std::collections::HashMap::new();
        let mut declared_schemas = std::collections::HashMap::new();
        for row in rows(&results, 1) {
            if row.get("space_id").and_then(Value::as_str) == Some(domain_id.as_str()) {
                if let Some(name) = row.get("name").and_then(Value::as_str) {
                    feeds.push(name.to_owned());
                    feed_schemas.insert(
                        name.to_owned(),
                        FeedSchema::from_wire(row.get("effective_schema")),
                    );
                    declared_schemas
                        .insert(name.to_owned(), FeedSchema::from_wire(row.get("schema")));
                }
                if let Some(feed_id) = row.get("feed_id").and_then(Value::as_str) {
                    feed_ids.push(feed_id.to_owned());
                }
            }
        }
        feeds.sort();

        let space_ids = std::slice::from_ref(&domain_id);
        Ok(Snapshot {
            domain_id: domain_id.clone(),
            describe,
            feeds,
            feed_schemas,
            declared_schemas,
            readers: scoped(&results, 2, "feed_id", &feed_ids),
            writers: scoped(&results, 3, "feed_id", &feed_ids),
            subscriptions: scoped(&results, 4, "space_id", space_ids),
            pipes: scoped(&results, 5, "space_id", space_ids),
            indexes: scoped(&results, 6, "space_id", space_ids),
            state_stores: scoped(&results, 7, "space_id", space_ids),
        })
    }
}

/// Fluent declaration + open of a durable named Reader — the typed
/// companion to [`Client::create_reader`]. `reader_id` is the durable
/// consume identity (the Kafka `groupId` analogue): acknowledged
/// progress survives restarts and resumes under the same name.
/// `reader_start_point` applies only when the Reader is first declared —
/// an existing Reader resumes its acknowledged frontier, exactly like
/// Kafka's `auto.offset.reset`.
pub struct ReaderBuilder<K, V> {
    client: Client,
    feed: Feed<K, V>,
    reader_id: Option<String>,
    start: ReaderStart,
    max_page_size: u64,
}

impl<K, V> ReaderBuilder<K, V> {
    pub(crate) fn new(client: Client, feed: Feed<K, V>) -> Self {
        Self {
            client,
            feed,
            reader_id: None,
            start: ReaderStart::Beginning,
            max_page_size: DEFAULT_READER_CAPACITY,
        }
    }

    /// The durable Reader identity — required before
    /// [`ReaderBuilder::open`]/[`ReaderBuilder::declare`].
    pub fn reader_id(mut self, reader_id: &str) -> Self {
        self.reader_id = Some(reader_id.to_owned());
        self
    }

    /// First-time start position — [`ReaderStart::Beginning`],
    /// [`ReaderStart::Now`], `ReaderStart::Cursor`, or
    /// `ReaderStart::TimestampNs`. Ignored once the Reader exists.
    pub fn reader_start_point(mut self, start: ReaderStart) -> Self {
        self.start = start;
        self
    }

    /// Max records delivered per fetch page — the default is 128.
    pub fn max_page_size(mut self, max_page_size: u64) -> Self {
        self.max_page_size = max_page_size;
        self
    }

    /// Declare the Reader in the catalog without opening a session —
    /// fails when the name is already declared.
    pub async fn declare(&self) -> Result<Vec<Value>, SdkError> {
        self.client
            .wcl(&format!(
                "CREATE READER {} FROM {} START AT {};",
                self.require_id()?,
                self.feed.name(),
                self.start.wcl()
            ))
            .await
    }

    /// Declare the Reader when missing (an existing declaration keeps
    /// its own start and durable progress), open a session, and return a
    /// codec-bound [`FeedReader`].
    pub async fn open(&self) -> Result<FeedReader<K, V>, SdkError> {
        self.feed
            .open_reader(self.require_id()?, self.max_page_size, &self.start.wcl())
            .await
    }

    fn require_id(&self) -> Result<&str, SdkError> {
        self.reader_id.as_deref().ok_or_else(|| {
            SdkError::Refused(
                "reader_id(...) is required — the durable consume identity \
                 (the Kafka groupId analogue)"
                    .to_owned(),
            )
        })
    }
}

/// Fluent declaration + open of a named Writer — the typed companion to
/// [`Client::create_writer`]. `writer_id` is the durable produce
/// identity: dedup state the server tracks survives restarts under the
/// same name.
pub struct WriterBuilder<K, V> {
    client: Client,
    feed: Feed<K, V>,
    writer_id: Option<String>,
}

impl<K, V> WriterBuilder<K, V> {
    pub(crate) fn new(client: Client, feed: Feed<K, V>) -> Self {
        Self {
            client,
            feed,
            writer_id: None,
        }
    }

    /// The durable Writer identity — required before
    /// [`WriterBuilder::open`]/[`WriterBuilder::declare`].
    pub fn writer_id(mut self, writer_id: &str) -> Self {
        self.writer_id = Some(writer_id.to_owned());
        self
    }

    /// Declare the Writer in the catalog without opening a session —
    /// fails when the name is already declared.
    pub async fn declare(&self) -> Result<Vec<Value>, SdkError> {
        self.client
            .wcl(&format!(
                "CREATE WRITER {} TO {};",
                self.require_id()?,
                self.feed.name()
            ))
            .await
    }

    /// Declare the Writer when missing, open a session, and return a
    /// codec-bound [`FeedWriter`].
    pub async fn open(&self) -> Result<FeedWriter<K, V>, SdkError> {
        self.feed.writer(self.require_id()?).await
    }

    fn require_id(&self) -> Result<&str, SdkError> {
        self.writer_id.as_deref().ok_or_else(|| {
            SdkError::Refused(
                "writer_id(...) is required — the durable produce identity".to_owned(),
            )
        })
    }
}

/// `SHOW` rows for result `index` — a missing/malformed statement data
/// reads as an empty listing.
fn rows(results: &[Value], index: usize) -> Vec<Value> {
    results
        .get(index)
        .and_then(|result| result.get("data"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Sorted names from `SHOW` result `index` scoped to rows whose
/// `scope_field` is one of `scope_ids`.
fn scoped(results: &[Value], index: usize, scope_field: &str, scope_ids: &[String]) -> Vec<String> {
    let mut names: Vec<String> = rows(results, index)
        .iter()
        .filter(|row| {
            row.get(scope_field)
                .and_then(Value::as_str)
                .is_some_and(|scope| scope_ids.iter().any(|id| id == scope))
        })
        .filter_map(|row| row.get("name").and_then(Value::as_str).map(str::to_owned))
        .collect();
    names.sort();
    names
}

/// `shop.orders` minus the `shop.` prefix.
fn relative<'a>(domain: &str, full: &'a str) -> &'a str {
    full.strip_prefix(&format!("{domain}.")).unwrap_or(full)
}

/// Dotted-name grammar — lowercase segments
/// `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`, 512 bytes max.
fn dotted(name: &str) -> Result<String, SdkError> {
    let valid = !name.is_empty()
        && name.len() <= 512
        && name.split('.').all(|segment| {
            let mut chars = segment.chars();
            matches!(chars.next(), Some(first) if first.is_ascii_lowercase())
                && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        });
    if valid {
        Ok(name.to_owned())
    } else {
        Err(SdkError::Refused(format!("invalid dotted name: '{name}'")))
    }
}
