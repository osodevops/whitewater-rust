//! Declared StateStores: byte-level rows, freshness evidence, and
//! idempotent mutations. The codec-bound typed facade is `crate::Store`.

use base64::Engine;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

use crate::client::Client;
use crate::error::SdkError;
use crate::model::STANDARD;
use crate::typed::{wire_typed, Extractor, TypedValue};

/// The most named secondary keys one StateStore may declare — the
/// server enforces the same ceiling.
pub const MAX_NAMED_KEYS: usize = 10;

/// One declared secondary key on a StateStore. Non-unique keys map a
/// value to every primary row carrying it (the group/list lookup);
/// `unique` claims enforce one row per value, arbitrated
/// first-committed-wins by the store's journal order. `extractor`
/// projects values from a source-Feed record — feed-sourced stores only;
/// manual stores take values on each `put_row` mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedKey {
    pub name: String,
    pub unique: bool,
    pub extractor: Option<Extractor>,
}

/// The declaration `Client::define_state_store` commits: the store name
/// plus its named keys. Build with [`StateStoreSpec::new`] and chain
/// [`StateStoreSpec::add_key`] — named keys are declared at creation and
/// cannot be added to an already-declared store.
///
/// ```no_run
/// # use whitewater::{Client, StateStoreSpec, Extractor};
/// # async fn example(client: &Client) -> Result<(), whitewater::SdkError> {
/// let accounts = client.domain("accounts")?;
/// let spec = StateStoreSpec::new("employees")?     // -> accounts.employees
///     .add_unique_key("passport")?
///     .add_non_unique_key("department")?;
/// let store = accounts.define_state_store(spec, None).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct StateStoreSpec {
    name: String,
    named_keys: Vec<NamedKey>,
}

impl StateStoreSpec {
    /// An unbound declaration spec for [`Client::define_state_store`].
    pub fn new(name: impl Into<String>) -> Result<Self, SdkError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(SdkError::Refused("state store name is required".to_owned()));
        }
        Ok(Self {
            name,
            named_keys: Vec::new(),
        })
    }

    /// Declare a named secondary key (at most [`MAX_NAMED_KEYS`]).
    /// Non-unique keys map one value to every matching primary row;
    /// `unique` enforces one row per value. An `extractor` declares how
    /// replay projects each source-Feed record to this key's values —
    /// required on feed-sourced stores, refused on manual ones.
    pub fn add_key(
        mut self,
        named_key: impl Into<String>,
        unique: bool,
        extractor: Option<Extractor>,
    ) -> Result<Self, SdkError> {
        let named_key = named_key.into();
        if named_key.trim().is_empty() {
            return Err(SdkError::Refused("named key is required".to_owned()));
        }
        if self.named_keys.iter().any(|key| key.name == named_key) {
            return Err(SdkError::Refused(format!(
                "duplicate named key: {named_key}"
            )));
        }
        if self.named_keys.len() >= MAX_NAMED_KEYS {
            return Err(SdkError::Refused(format!(
                "a StateStore declares at most {MAX_NAMED_KEYS} named keys"
            )));
        }
        self.named_keys.push(NamedKey {
            name: named_key,
            unique,
            extractor,
        });
        Ok(self)
    }

    /// Declare a **unique** named key — one value resolves to at most
    /// one primary row (`add_key(named_key, true, None)`). For
    /// feed-sourced keys use [`add_unique_key_with`](Self::add_unique_key_with).
    pub fn add_unique_key(self, named_key: impl Into<String>) -> Result<Self, SdkError> {
        self.add_key(named_key, true, None)
    }

    /// Declare a **unique** named key with an [`Extractor`] — feed-sourced
    /// stores only: replay projects each source-Feed record to this
    /// key's values (`add_key(named_key, true, Some(extractor))`).
    pub fn add_unique_key_with(
        self,
        named_key: impl Into<String>,
        extractor: Extractor,
    ) -> Result<Self, SdkError> {
        self.add_key(named_key, true, Some(extractor))
    }

    /// Declare a **non-unique** named key — one value lists every
    /// matching primary row (`add_key(named_key, false, None)`). For
    /// feed-sourced keys use [`add_non_unique_key_with`](Self::add_non_unique_key_with).
    pub fn add_non_unique_key(self, named_key: impl Into<String>) -> Result<Self, SdkError> {
        self.add_key(named_key, false, None)
    }

    /// Declare a **non-unique** named key with an [`Extractor`] —
    /// feed-sourced stores only (`add_key(named_key, false, Some(extractor))`).
    pub fn add_non_unique_key_with(
        self,
        named_key: impl Into<String>,
        extractor: Extractor,
    ) -> Result<Self, SdkError> {
        self.add_key(named_key, false, Some(extractor))
    }

    /// The declared named keys in declaration order.
    pub fn named_keys(&self) -> &[NamedKey] {
        &self.named_keys
    }

    /// The store name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Outcome of one committed StateStore mutation.
#[derive(Clone, Debug)]
pub struct MutationOutcome {
    pub store_id: Uuid,
    pub sequence: u64,
    /// Identical `mutation_id` replay — the row did not commit twice.
    pub deduplicated: bool,
}

/// One row as served by lookup/get_by/iterate.
#[derive(Clone, Debug)]
pub struct StoreRow {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Freshness + identity envelope every StateStore read reports.
#[derive(Clone, Debug)]
pub struct ReadFreshness {
    /// Highest journal sequence the serving replica applied.
    pub applied_seq: u64,
    /// Highest committed sequence on the serving replica.
    pub committed_seq: u64,
    /// `applied_seq < committed_seq`: the replica is still applying.
    pub behind: bool,
    /// Node identity that served — diagnostics, never routing input.
    pub served_by: String,
}

#[derive(Clone, Debug)]
pub struct LookupOutcome {
    pub row: Option<StoreRow>,
    pub freshness: ReadFreshness,
}

#[derive(Clone, Debug)]
pub struct GetByPage {
    pub rows: Vec<StoreRow>,
    pub freshness: ReadFreshness,
}

#[derive(Clone, Debug)]
pub struct IteratePage {
    pub rows: Vec<StoreRow>,
    /// Continuation key for the next page; `None` at the end.
    pub next_key: Option<Vec<u8>>,
    /// Journal position the page was pinned to.
    pub pinned_seq: u64,
    pub served_by: String,
}

/// Outcome of `sync`/`rebuild`: records applied this pass plus whether
/// the store reached the committed tip of its source Feed.
#[derive(Clone, Debug)]
pub struct SyncOutcome {
    pub applied: u64,
    pub frontier: Option<String>,
    pub complete: bool,
}

fn decode_store_row(value: &Value, key_field: &str) -> Result<StoreRow, SdkError> {
    let decode = |field: &str| -> Result<Vec<u8>, SdkError> {
        value
            .get(field)
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok())
            .ok_or_else(|| SdkError::Protocol(format!("store row missing {field}")))
    };
    Ok(StoreRow {
        key: decode(key_field)?,
        payload: decode("payload_base64")?,
    })
}

fn decode_freshness(body: &Value) -> ReadFreshness {
    ReadFreshness {
        applied_seq: body.get("applied_seq").and_then(Value::as_u64).unwrap_or(0),
        committed_seq: body
            .get("committed_seq")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        behind: body.get("behind").and_then(Value::as_bool).unwrap_or(false),
        served_by: body
            .get("served_by")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    }
}

fn decode_mutation_outcome(body: &Value) -> Result<MutationOutcome, SdkError> {
    Ok(MutationOutcome {
        store_id: body
            .get("store_id")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .ok_or_else(|| SdkError::Protocol("mutation returned no store_id".to_owned()))?,
        sequence: body.get("sequence").and_then(Value::as_u64).unwrap_or(0),
        deduplicated: body
            .get("deduplicated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn decode_sync_outcome(body: &Value) -> SyncOutcome {
    SyncOutcome {
        applied: body.get("applied").and_then(Value::as_u64).unwrap_or(0),
        frontier: body
            .get("frontier")
            .and_then(Value::as_str)
            .map(str::to_owned),
        complete: body
            .get("complete")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

/// A declared StateStore bound by name. Manual stores accept
/// `put_row`/`delete_row`/`clear` under caller `mutation_id`s (identical
/// retries dedupe); feed-sourced stores refuse mutation server-side and
/// are maintained by `sync`/`rebuild`.
/// Strongly-typed or inline secondary-key postings for a [`StateStore`]
/// or [`crate::Store`] mutation. Wrap [`StoreKeyMap`] inside a domain
/// struct to give your store typed fluent builders and `with(...)`
/// constructors.
pub trait StoreKeySet {
    /// Lower this key set into the `(name, values)` pairs carried on the wire.
    fn to_named_postings(&self) -> Vec<(String, Vec<TypedValue>)>;
}

impl<T: StoreKeySet + ?Sized> StoreKeySet for &T {
    fn to_named_postings(&self) -> Vec<(String, Vec<TypedValue>)> {
        (**self).to_named_postings()
    }
}

impl StoreKeySet for [(String, Vec<TypedValue>)] {
    fn to_named_postings(&self) -> Vec<(String, Vec<TypedValue>)> {
        self.to_vec()
    }
}

impl StoreKeySet for Vec<(String, Vec<TypedValue>)> {
    fn to_named_postings(&self) -> Vec<(String, Vec<TypedValue>)> {
        self.clone()
    }
}

/// Composable base map for typed [`StoreKeySet`] implementations.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StoreKeyMap {
    keys: Vec<(String, Vec<TypedValue>)>,
}

impl StoreKeyMap {
    pub fn new() -> Self {
        Self { keys: Vec::new() }
    }

    pub fn of(builder: impl FnOnce(&mut Self)) -> Self {
        let mut ks = Self::new();
        builder(&mut ks);
        ks
    }

    pub fn put(mut self, name: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        self.set(name, value);
        self
    }

    pub fn set(&mut self, name: impl Into<String>, value: impl Into<TypedValue>) -> &mut Self {
        let name = name.into();
        let value = value.into();
        if let Some((_, existing)) = self.keys.iter_mut().find(|(k, _)| k == &name) {
            *existing = vec![value];
        } else {
            self.keys.push((name, vec![value]));
        }
        self
    }

    pub fn get(&self, name: &str) -> Option<&[TypedValue]> {
        self.keys
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_slice())
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl StoreKeySet for StoreKeyMap {
    fn to_named_postings(&self) -> Vec<(String, Vec<TypedValue>)> {
        self.keys.clone()
    }
}

/// Read options for store lookups and pages — a builder so every knob
/// names itself at the call site:
/// `store.get_by_opts("department", values, &ReadOpts::new().strict().limit(64))`.
#[derive(Clone, Debug, Default)]
pub struct ReadOpts {
    pub(crate) strict: bool,
    pub(crate) wait_ms: Option<u64>,
    pub(crate) limit: Option<u64>,
    pub(crate) after: Option<Vec<u8>>,
}

impl ReadOpts {
    /// Fresh options — non-strict, unbounded.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bounded-wait for the serving replica to apply its committed
    /// frontier rather than answering stale.
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Bound the strict wait — `wait_ms` applies to strict reads, so
    /// `wait` implies [`Self::strict`].
    pub fn wait(mut self, wait: Duration) -> Self {
        self.strict = true;
        self.wait_ms = Some(wait.as_millis() as u64);
        self
    }

    /// Cap the rows a `get_by` or `iterate` page returns.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Continue an `iterate` page after a previous page's `next_key`.
    pub fn after(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.after = Some(key.into());
        self
    }
}

/// A mutation-id-scoped view: every mutation carries the caller's
/// idempotency identity so a retried sequence dedupes (and a different
/// payload under the same id conflicts):
/// `store.with_mutation_id(id).put_row(key, payload, named)`.
/// The unscoped methods generate one id per call.
pub struct MutationScope<'a, S> {
    pub(crate) store: &'a S,
    pub(crate) mutation_id: Uuid,
}

fn declared_default_keys() -> &'static std::sync::Mutex<std::collections::HashMap<String, String>> {
    static MAP: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, String>>> =
        std::sync::OnceLock::new();
    MAP.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

pub(crate) fn register_default_key(store_name: &str, default_key: &str) {
    if !store_name.trim().is_empty() && !default_key.trim().is_empty() {
        if let Ok(mut guard) = declared_default_keys().lock() {
            guard.insert(store_name.to_owned(), default_key.to_owned());
        }
    }
}

#[derive(Clone)]
pub struct StateStore {
    pub(crate) client: Client,
    pub(crate) name: String,
    pub(crate) default_key: Option<String>,
}

impl StateStore {
    /// Bind the default secondary-key name used by [`Self::put_with_key`].
    pub fn with_default_key(mut self, default_key: impl Into<String>) -> Self {
        self.default_key = Some(default_key.into());
        self
    }

    /// Scope mutations under a caller-supplied idempotency identity —
    /// a retried sequence dedupes, a different payload under the same
    /// id conflicts. The unscoped methods generate one id per call.
    pub fn with_mutation_id(&self, mutation_id: Uuid) -> MutationScope<'_, Self> {
        MutationScope {
            store: self,
            mutation_id,
        }
    }

    pub(crate) fn resolve_default_key_name(&self) -> String {
        if let Some(key) = &self.default_key {
            return key.clone();
        }
        if let Ok(guard) = declared_default_keys().lock() {
            if let Some(key) = guard.get(&self.name) {
                return key.clone();
            }
        }
        "default".to_owned()
    }

    /// Put one row with a typed [`StoreKeySet`]; the mutation id
    /// generates per call — [`Self::with_mutation_id`] binds a stable
    /// one for retry-safe sequences.
    pub async fn put_key_set(
        &self,
        key: &[u8],
        payload: &[u8],
        keys: &impl StoreKeySet,
    ) -> Result<MutationOutcome, SdkError> {
        let named = keys.to_named_postings();
        self.put_row_with_id(Uuid::new_v4(), key, payload, &named)
            .await
    }

    /// Put one row with a single default secondary-key string (mapped to the store's first declared key).
    pub async fn put_with_key(
        &self,
        key: &[u8],
        payload: &[u8],
        single_secondary_key: impl Into<String>,
    ) -> Result<MutationOutcome, SdkError> {
        let named = vec![(
            self.resolve_default_key_name(),
            vec![TypedValue::Text(single_secondary_key.into())],
        )];
        self.put_row_with_id(Uuid::new_v4(), key, payload, &named)
            .await
    }
    /// Put one row with optional named-key postings; the mutation id
    /// generates per call — [`Self::with_mutation_id`] binds a stable
    /// one for retry-safe sequences.
    pub async fn put_row(
        &self,
        key: &[u8],
        payload: &[u8],
        named: &[(String, Vec<TypedValue>)],
    ) -> Result<MutationOutcome, SdkError> {
        self.put_row_with_id(Uuid::new_v4(), key, payload, named)
            .await
    }

    /// Put one row under an explicit mutation id — the idempotency
    /// identity: an identical retry dedupes, a different payload under
    /// the same id conflicts.
    pub async fn put_row_with_id(
        &self,
        mutation_id: Uuid,
        key: &[u8],
        payload: &[u8],
        named: &[(String, Vec<TypedValue>)],
    ) -> Result<MutationOutcome, SdkError> {
        let named_wire: Vec<Value> = named
            .iter()
            .map(|(name, values)| {
                json!({
                    "name": name,
                    "values": values.iter().map(wire_typed).collect::<Vec<_>>(),
                })
            })
            .collect();
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/mutate",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                    "op": {
                        "kind": "put_row",
                        "key_base64": STANDARD.encode(key),
                        "payload_base64": STANDARD.encode(payload),
                        "named": named_wire,
                    },
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Delete one row by primary key; the mutation id generates per
    /// call — [`Self::with_mutation_id`] binds a stable one.
    pub async fn delete_row(&self, key: &[u8]) -> Result<MutationOutcome, SdkError> {
        self.delete_row_with_id(Uuid::new_v4(), key).await
    }

    /// Delete one row by primary key under an explicit `mutation_id`
    /// idempotency identity.
    pub async fn delete_row_with_id(
        &self,
        mutation_id: Uuid,
        key: &[u8],
    ) -> Result<MutationOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/mutate",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                    "op": {
                        "kind": "delete_row",
                        "key_base64": STANDARD.encode(key),
                    },
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Empty the store; the mutation id generates per call —
    /// [`Self::with_mutation_id`] binds a stable one.
    pub async fn clear(&self) -> Result<MutationOutcome, SdkError> {
        self.clear_with_id(Uuid::new_v4()).await
    }

    /// Empty the store under an explicit `mutation_id` identity —
    /// replay dedupes.
    pub async fn clear_with_id(&self, mutation_id: Uuid) -> Result<MutationOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/clear",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Point lookup by primary key — non-strict. [`Self::lookup_strict`]
    /// bounded-waits for the replica's committed snapshot; unreachable
    /// freshness answers `IndexBehind`.
    pub async fn lookup(&self, key: &[u8]) -> Result<LookupOutcome, SdkError> {
        self.lookup_opts(key, &ReadOpts::new()).await
    }

    /// Point lookup that bounded-waits for the serving replica to apply
    /// its committed frontier rather than answering stale.
    pub async fn lookup_strict(&self, key: &[u8]) -> Result<LookupOutcome, SdkError> {
        self.lookup_opts(key, &ReadOpts::new().strict()).await
    }

    /// The full lookup primitive — [`ReadOpts::strict`], a bounded
    /// [`ReadOpts::wait`], or both.
    pub async fn lookup_opts(
        &self,
        key: &[u8],
        opts: &ReadOpts,
    ) -> Result<LookupOutcome, SdkError> {
        let mut request = json!({
            "store": self.name,
            "key_base64": STANDARD.encode(key),
            "strict": opts.strict,
        });
        if let Some(wait_ms) = opts.wait_ms {
            request["wait_ms"] = json!(wait_ms);
        }
        let body = self
            .client
            .call("POST", "/v1/state-stores/lookup", Some(&request))
            .await?;
        let row = match body.get("found").and_then(Value::as_bool) {
            Some(true) => Some(StoreRow {
                key: key.to_vec(),
                payload: body
                    .get("payload_base64")
                    .and_then(Value::as_str)
                    .and_then(|raw| STANDARD.decode(raw).ok())
                    .ok_or_else(|| {
                        SdkError::Protocol("lookup found a row without payload".to_owned())
                    })?,
            }),
            _ => None,
        };
        Ok(LookupOutcome {
            row,
            freshness: decode_freshness(&body),
        })
    }

    /// Rows whose declared `named_key` posting equals one of `values` —
    /// the group/list lookup. [`Self::get_by_opts`] adds a row `limit`
    /// and the same strict freshness contract as `lookup`.
    pub async fn get_by(
        &self,
        named_key: &str,
        values: &[TypedValue],
    ) -> Result<GetByPage, SdkError> {
        self.get_by_opts(named_key, values, &ReadOpts::new()).await
    }

    /// The full `get_by` primitive — [`ReadOpts::limit`] caps the page,
    /// [`ReadOpts::strict`]/[`ReadOpts::wait`] bound freshness.
    pub async fn get_by_opts(
        &self,
        named_key: &str,
        values: &[TypedValue],
        opts: &ReadOpts,
    ) -> Result<GetByPage, SdkError> {
        let mut request = json!({
            "store": self.name,
            "named_key": named_key,
            "values": values.iter().map(wire_typed).collect::<Vec<_>>(),
            "limit": opts.limit,
            "strict": opts.strict,
        });
        if let Some(wait_ms) = opts.wait_ms {
            request["wait_ms"] = json!(wait_ms);
        }
        let body = self
            .client
            .call("POST", "/v1/state-stores/get-by", Some(&request))
            .await?;
        let rows = body
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|row| decode_store_row(row, "key_base64"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GetByPage {
            rows,
            freshness: decode_freshness(&body),
        })
    }

    /// First primary-key ordered page; [`Self::iterate_from`] continues
    /// a prior page's `next_key`, [`Self::iterate_opts`] adds `limit`.
    pub async fn iterate(&self) -> Result<IteratePage, SdkError> {
        self.iterate_opts(&ReadOpts::new()).await
    }

    /// Continue iteration from a prior page's `next_key`.
    pub async fn iterate_from(&self, after_key: &[u8]) -> Result<IteratePage, SdkError> {
        self.iterate_opts(&ReadOpts::new().after(after_key)).await
    }

    /// The full iterate primitive — [`ReadOpts::after`] resumes a page,
    /// [`ReadOpts::limit`] bounds it.
    pub async fn iterate_opts(&self, opts: &ReadOpts) -> Result<IteratePage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/iterate",
                Some(&json!({
                    "store": self.name,
                    "after_key_base64": opts.after.as_ref().map(|key| STANDARD.encode(key)),
                    "limit": opts.limit,
                })),
            )
            .await?;
        let rows = body
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|row| decode_store_row(row, "key_base64"))
            .collect::<Result<Vec<_>, _>>()?;
        let next_key = body
            .get("next_key_base64")
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok());
        Ok(IteratePage {
            rows,
            next_key,
            pinned_seq: body.get("pinned_seq").and_then(Value::as_u64).unwrap_or(0),
            served_by: body
                .get("served_by")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        })
    }

    /// Drive one catch-up pass on a feed-sourced store; `complete` means
    /// the committed tip was reached.
    pub async fn sync(&self) -> Result<SyncOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/sync",
                Some(&json!({ "store": self.name })),
            )
            .await?;
        Ok(decode_sync_outcome(&body))
    }

    /// Rebuild a feed-sourced store from retained history.
    /// [`Self::rebuild_with`] carries a caller retry token for the pass.
    pub async fn rebuild(&self) -> Result<SyncOutcome, SdkError> {
        self.rebuild_body(None).await
    }

    /// Rebuild with an explicit retry `token` — the same token marks a
    /// resumed pass.
    pub async fn rebuild_with(&self, token: Uuid) -> Result<SyncOutcome, SdkError> {
        self.rebuild_body(Some(token)).await
    }

    async fn rebuild_body(&self, token: Option<Uuid>) -> Result<SyncOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/rebuild",
                Some(&json!({
                    "store": self.name,
                    "token": token.map(|value| value.to_string()),
                })),
            )
            .await?;
        Ok(decode_sync_outcome(&body))
    }
}

impl<'a> MutationScope<'a, StateStore> {
    /// Put one row under the scope's idempotency identity.
    pub async fn put_row(
        &self,
        key: &[u8],
        payload: &[u8],
        named: &[(String, Vec<TypedValue>)],
    ) -> Result<MutationOutcome, SdkError> {
        self.store
            .put_row_with_id(self.mutation_id, key, payload, named)
            .await
    }

    /// Put one row with a typed [`StoreKeySet`] under the scope's
    /// idempotency identity.
    pub async fn put_key_set(
        &self,
        key: &[u8],
        payload: &[u8],
        keys: &impl StoreKeySet,
    ) -> Result<MutationOutcome, SdkError> {
        let named = keys.to_named_postings();
        self.put_row(key, payload, &named).await
    }

    /// Put one row with a single default secondary-key string under the
    /// scope's idempotency identity.
    pub async fn put_with_key(
        &self,
        key: &[u8],
        payload: &[u8],
        single_secondary_key: impl Into<String>,
    ) -> Result<MutationOutcome, SdkError> {
        let named = vec![(
            self.store.resolve_default_key_name(),
            vec![TypedValue::Text(single_secondary_key.into())],
        )];
        self.put_row(key, payload, &named).await
    }

    /// Delete one row under the scope's idempotency identity.
    pub async fn delete_row(&self, key: &[u8]) -> Result<MutationOutcome, SdkError> {
        self.store.delete_row_with_id(self.mutation_id, key).await
    }

    /// Empty the store under the scope's idempotency identity.
    pub async fn clear(&self) -> Result<MutationOutcome, SdkError> {
        self.store.clear_with_id(self.mutation_id).await
    }
}
