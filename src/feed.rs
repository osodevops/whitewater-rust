//! Codec-bound typed Feeds: `Feed<K, V>` carries key/value codecs once
//! and every write and read serializes automatically — the same
//! contract the other five SDKs expose.
//!
//! ```no_run
//! # async fn demo() -> Result<(), whitewater::SdkError> {
//! use whitewater::{Client, Codec, HttpTransport};
//! use std::sync::Arc;
//! let client = Client::new(
//!     Arc::new(HttpTransport::new()?),
//!     vec!["https://node1:7371".to_owned()],
//!     "api-key",
//! )?;
//! let orders = client.feed_with(
//!     "orders.events",
//!     Codec::utf8(),
//!     Codec::<serde_json::Value>::json(),
//! );
//! let writer = orders.writer("checkout").await?;
//! writer.append(&"order-1".to_owned(), &serde_json::json!({"total": 42})).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::future::Future;

use uuid::Uuid;

use crate::client::Client;
use crate::codec::Codec;
use crate::error::SdkError;
use crate::model::{now_nanos, AppendOutcome, Record, TemporaryFetch, TemporaryPage};
use crate::reader::Reader;
use crate::writer::Writer;

/// One delivered record with key and payload decoded through the bound
/// codecs. `key` is `None` for keyless records; cursors stay opaque.
#[derive(Clone, Debug)]
pub struct FeedMessage<K, V> {
    pub key: Option<K>,
    pub value: V,
    pub message_id: Option<Uuid>,
    pub cursor: Option<String>,
    pub event_time_ns: i64,
    pub ingest_time_ns: i64,
    /// Record Metadata as raw bytes — transport headers are a different
    /// concept and never appear here.
    pub metadata: BTreeMap<String, Vec<u8>>,
}

impl<K, V> FeedMessage<K, V> {
    /// One Metadata entry decoded as UTF-8 text, when present and valid.
    pub fn metadata_text(&self, name: &str) -> Option<String> {
        self.metadata
            .get(name)
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
    }
}

/// A record to append. `request_id` pins the idempotency identity —
/// supply one to drive identical retries explicitly; `None` generates
/// a fresh identity per call (the safe default for new records).
/// `event_time_ns` defaults to the append time when omitted.
#[derive(Clone, Debug)]
pub struct FeedEvent<K, V> {
    /// `None` writes a keyless record.
    pub key: Option<K>,
    pub value: V,
    pub request_id: Option<Uuid>,
    /// Signed Unix epoch nanoseconds; `None` uses append time.
    pub event_time_ns: Option<i64>,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

impl<K, V> FeedEvent<K, V> {
    /// The common case: keyed record, fresh identity, current time.
    pub fn new(key: K, value: V) -> Self {
        Self {
            key: Some(key),
            value,
            request_id: None,
            event_time_ns: None,
            metadata: BTreeMap::new(),
        }
    }

    /// Pin the dedup identity for caller-driven identical retries.
    pub fn with_request_id(mut self, request_id: Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }

    /// Pin the business event time (signed epoch nanoseconds).
    pub fn with_event_time_ns(mut self, event_time_ns: i64) -> Self {
        self.event_time_ns = Some(event_time_ns);
        self
    }

    /// A keyless record — no ordering key; spreads across the Feed.
    pub fn keyless(value: V) -> Self {
        Self {
            key: None,
            value,
            request_id: None,
            event_time_ns: None,
            metadata: BTreeMap::new(),
        }
    }

    /// UTF-8 convenience for building `metadata` maps.
    pub fn text_metadata(metadata: &[(&str, &str)]) -> BTreeMap<String, Vec<u8>> {
        metadata
            .iter()
            .map(|(name, value)| (name.to_string(), value.as_bytes().to_vec()))
            .collect()
    }
}

/// A page delivered by `FeedReader::fetch` — decoded records plus the
/// progress cursors `ack` commits.
#[derive(Clone, Debug)]
pub struct FeedReaderPage<K, V> {
    pub records: Vec<FeedMessage<K, V>>,
    /// Advances when this page lands; `ack` commits this cursor.
    pub delivered_cursor: Option<String>,
    pub acknowledged_cursor: Option<String>,
    pub recommended_capacity: Option<u64>,
    pub retry_after_ms: Option<u64>,
}

/// A decoded page from `Feed::fetch` (bounded history walk or tail).
#[derive(Clone, Debug)]
pub struct FeedPage<K, V> {
    pub records: Vec<FeedMessage<K, V>>,
    pub next_cursor: Option<String>,
}

/// A declared Writer on this Feed with codecs applied. Identical
/// `request_id` retries dedupe server-side.
pub struct FeedWriter<K, V> {
    session: Writer,
    key_codec: Codec<K>,
    value_codec: Codec<V>,
}

impl<K, V> FeedWriter<K, V> {
    /// The session's fencing epoch.
    pub fn session_epoch(&self) -> u64 {
        self.session.session_epoch()
    }

    /// Reopen after `StaleEpoch` — the server supersedes the old epoch.
    pub async fn reopen(&mut self) -> Result<(), SdkError> {
        self.session.reopen().await
    }

    /// Encode and append one keyed record; request identity and event
    /// time auto-generate. For keyless records or pinned identity use
    /// `append_event`.
    pub async fn append(&self, key: &K, value: &V) -> Result<AppendOutcome, SdkError> {
        let key_bytes = self.key_codec.encode(key)?;
        let payload = self.value_codec.encode(value)?;
        self.session
            .append(
                Uuid::new_v4(),
                &key_bytes,
                &payload,
                now_nanos(),
                BTreeMap::new(),
            )
            .await
    }

    /// Append one typed event — pin `request_id` for caller-driven
    /// retries (identical retries dedupe server-side).
    pub async fn append_event(&self, event: FeedEvent<K, V>) -> Result<AppendOutcome, SdkError> {
        let (request_id, key, payload, event_time_ns) = self.wire(&event)?;
        self.session
            .append(
                request_id,
                &key,
                &payload,
                event_time_ns,
                event.metadata.clone(),
            )
            .await
    }

    /// Append a batch as one epoch-fenced request; per-record
    /// `request_id`s keep the same retry contract as `append_event`.
    pub async fn append_batch(
        &self,
        events: &[FeedEvent<K, V>],
    ) -> Result<Vec<AppendOutcome>, SdkError> {
        let mut items = Vec::with_capacity(events.len());
        for event in events {
            let (request_id, key, payload, event_time_ns) = self.wire(event)?;
            items.push((request_id, key, payload, event_time_ns));
        }
        self.session.append_batch(&items).await
    }

    fn wire(&self, event: &FeedEvent<K, V>) -> Result<(Uuid, Vec<u8>, Vec<u8>, i64), SdkError> {
        let key = match &event.key {
            Some(key) => self.key_codec.encode(key)?,
            None => Vec::new(),
        };
        let payload = self.value_codec.encode(&event.value)?;
        Ok((
            event.request_id.unwrap_or_else(Uuid::new_v4),
            key,
            payload,
            event.event_time_ns.unwrap_or_else(now_nanos),
        ))
    }
}

/// A session-bound named Reader on this Feed: `fetch` returns decoded
/// pages; `ack` commits the page's delivered cursor durably.
pub struct FeedReader<K, V> {
    session: Reader,
    key_codec: Codec<K>,
    value_codec: Codec<V>,
}

impl<K, V> FeedReader<K, V> {
    /// The session's fencing epoch.
    pub fn session_epoch(&self) -> u64 {
        self.session.session_epoch()
    }

    /// The durable restart point after the last `ack`.
    pub fn acknowledged_cursor(&self) -> Option<&str> {
        self.session.acknowledged_cursor()
    }

    /// Fetch one bounded page of decoded records. Ack the page's
    /// `delivered_cursor` via `ack` — never a record cursor.
    pub async fn fetch(&mut self, limit: u64) -> Result<FeedReaderPage<K, V>, SdkError> {
        let page = self.session.fetch(limit).await?;
        Ok(FeedReaderPage {
            records: decode_messages(&page.records, &self.key_codec, &self.value_codec)?,
            delivered_cursor: page.delivered_cursor,
            acknowledged_cursor: page.acknowledged_cursor,
            recommended_capacity: page.recommended_capacity,
            retry_after_ms: page.retry_after_ms,
        })
    }

    /// Commit the latest fetched page's delivered cursor.
    pub async fn ack(&mut self) -> Result<(), SdkError> {
        self.session.ack().await
    }

    /// Close the session; acknowledged progress stays durable.
    pub async fn close(self) -> Result<(), SdkError> {
        self.session.close().await
    }
}

/// Handle for a running `Feed::subscribe` consumer task. `stop`
/// cancels the poll loop; `wait` joins it and surfaces the terminal
/// error (a stopped subscription reports the last failure or `None`).
pub struct Subscription {
    task: tokio::task::JoinHandle<Result<(), SdkError>>,
}

impl Subscription {
    /// Abort the poll loop. In-flight handlers finish; acknowledged
    /// progress is already durable.
    pub fn stop(&self) {
        self.task.abort();
    }

    /// Join the consumer task — returns its terminal error, or `Ok(())`
    /// after a clean `stop`. Awaiting a stopped task is immediate.
    pub async fn wait(self) -> Result<(), SdkError> {
        match self.task.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(SdkError::Transport(format!("subscription task: {error}"))),
        }
    }
}

/// A codec-bound declared Feed. `Client::feed` binds the default
/// utf8-key/bytes-payload codecs; `Client::feed_with` binds explicit
/// codecs; `view` rebinds without a wire call.
pub struct Feed<K, V> {
    pub(crate) client: Client,
    pub(crate) name: String,
    pub(crate) key_codec: Codec<K>,
    pub(crate) value_codec: Codec<V>,
}

// Manual Clone so the codecs' unbounded Clone carries through —
/// K/V need no Clone bound themselves.
impl<K, V> Clone for Feed<K, V> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            name: self.name.clone(),
            key_codec: self.key_codec.clone(),
            value_codec: self.value_codec.clone(),
        }
    }
}

impl<K, V> Feed<K, V> {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The bound key codec.
    pub fn key_codec(&self) -> &Codec<K> {
        &self.key_codec
    }

    /// The bound value codec.
    pub fn value_codec(&self) -> &Codec<V> {
        &self.value_codec
    }

    /// Rebind codecs without a wire call — a view over the same Feed.
    pub fn view<KK, VV>(&self, key_codec: Codec<KK>, value_codec: Codec<VV>) -> Feed<KK, VV> {
        Feed {
            client: self.client.clone(),
            name: self.name.clone(),
            key_codec,
            value_codec,
        }
    }

    /// Session-bound named Writer on this Feed: declares `CREATE WRITER
    /// <name> TO <feed>` when missing, then opens the session epoch.
    pub async fn writer(&self, writer_name: &str) -> Result<FeedWriter<K, V>, SdkError> {
        self.client
            .create_resource_tolerating_exists(&format!(
                "CREATE WRITER {writer_name} TO {}",
                self.name
            ))
            .await?;
        let session = self.client.writer(writer_name).await?;
        Ok(FeedWriter {
            session,
            key_codec: self.key_codec.clone(),
            value_codec: self.value_codec.clone(),
        })
    }

    /// Session-bound named Reader on this Feed: declares `CREATE READER
    /// <name> FROM <feed> START AT BEGINNING` when missing, then opens a
    /// session with the bounded fetch `capacity`.
    pub async fn reader(
        &self,
        reader_name: &str,
        capacity: u64,
    ) -> Result<FeedReader<K, V>, SdkError> {
        self.open_reader(reader_name, capacity, "BEGINNING").await
    }

    /// Declare the Reader at `start_wcl` when missing, then open — the
    /// [`FeedConsumer`](crate::consumer::FeedConsumer) seam.
    pub(crate) async fn open_reader(
        &self,
        reader_name: &str,
        capacity: u64,
        start_wcl: &str,
    ) -> Result<FeedReader<K, V>, SdkError> {
        self.client
            .create_resource_tolerating_exists(&format!(
                "CREATE READER {reader_name} FROM {} START AT {start_wcl}",
                self.name
            ))
            .await?;
        let session = self.client.reader(reader_name, capacity).await?;
        Ok(FeedReader {
            session,
            key_codec: self.key_codec.clone(),
            value_codec: self.value_codec.clone(),
        })
    }

    /// A permanent, supervised consumer bound to this Feed's named
    /// Reader — pull decoded records via
    /// [`FeedConsumer::next`](crate::consumer::FeedConsumer::next) or a
    /// per-record `run` handler; open/fetch/ack/reopen/backoff stay
    /// inside the SDK. See [`crate::consumer`].
    ///
    /// ```ignore
    /// let mut consumer = feed.consumer("enricher");
    /// while let Some(message) = consumer.next().await? {
    ///     process(&message).await;
    /// }
    /// ```
    pub fn consumer(&self, reader_name: &str) -> crate::consumer::FeedConsumer<K, V>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        crate::consumer::FeedConsumer::new(self.clone(), reader_name)
    }

    /// Read one page straight off the Feed: `after` resumes from an
    /// opaque Cursor, `None` reads from the beginning.
    pub async fn records(
        &self,
        after: Option<&str>,
        limit: u64,
    ) -> Result<Vec<FeedMessage<K, V>>, SdkError> {
        let records = self.client.records(&self.name, after, limit).await?;
        decode_messages(&records, &self.key_codec, &self.value_codec)
    }

    /// Bounded history from the beginning — the `records` convenience
    /// for one-shot reads.
    pub async fn load(&self, limit: u64) -> Result<Vec<FeedMessage<K, V>>, SdkError> {
        self.records(None, limit).await
    }

    /// Temporary read without declaring a Reader — bounded history
    /// walks, live tails, or fresh-only waits per `options`.
    pub async fn fetch(&self, options: TemporaryFetch) -> Result<FeedPage<K, V>, SdkError> {
        let TemporaryPage {
            records,
            next_cursor,
        } = self.client.temporary_fetch(&self.name, options).await?;
        Ok(FeedPage {
            records: decode_messages(&records, &self.key_codec, &self.value_codec)?,
            next_cursor,
        })
    }

    /// A running consumer on this Feed: declares/opens a named Reader,
    /// then polls `fetch` → `handler` → `ack` in a background task.
    /// Progress commits after each successful handler call; a handler
    /// error ends the task and `Subscription::wait` surfaces it —
    /// unacked work redelivers on the next subscription.
    pub async fn subscribe<F, Fut>(
        &self,
        reader_name: &str,
        capacity: u64,
        limit: u64,
        mut handler: F,
    ) -> Result<Subscription, SdkError>
    where
        K: Send + 'static,
        V: Send + 'static,
        F: FnMut(Vec<FeedMessage<K, V>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), SdkError>> + Send + 'static,
    {
        let mut reader = self.reader(reader_name, capacity).await?;
        let task = tokio::spawn(async move {
            loop {
                let page = reader.fetch(limit).await?;
                if page.records.is_empty() {
                    continue;
                }
                handler(page.records).await?;
                reader.ack().await?;
            }
        });
        Ok(Subscription { task })
    }
}

pub(crate) fn decode_messages<K, V>(
    records: &[Record],
    key_codec: &Codec<K>,
    value_codec: &Codec<V>,
) -> Result<Vec<FeedMessage<K, V>>, SdkError> {
    records
        .iter()
        .map(|record| {
            Ok(FeedMessage {
                key: if record.key.is_empty() {
                    None
                } else {
                    Some(key_codec.decode(&record.key)?)
                },
                value: value_codec.decode(&record.payload)?,
                message_id: record.message_id,
                cursor: record.cursor.clone(),
                event_time_ns: record.event_time_ns,
                ingest_time_ns: record.ingest_time_ns,
                metadata: record.metadata.clone(),
            })
        })
        .collect()
}
