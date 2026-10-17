//! Codec-bound typed StateStores: `Store<K, V>` serializes keys and
//! row payloads through bound codecs on every get/put/iterate — the
//! same convenience contract as typed Feeds.

use uuid::Uuid;

use crate::codec::Codec;
use crate::error::SdkError;
use crate::store::{MutationOutcome, ReadFreshness, StateStore, SyncOutcome};
use crate::typed::TypedValue;

/// One decoded row.
#[derive(Clone, Debug)]
pub struct Row<K, V> {
    pub key: K,
    pub value: V,
}

/// Decoded page carrying the read's freshness evidence verbatim — the
/// same `applied_seq`/`committed_seq`/`behind`/`served_by` envelope the
/// byte-level API reports, because codec convenience never hides
/// correctness signals.
#[derive(Clone, Debug)]
pub struct RowPage<K, V> {
    pub rows: Vec<Row<K, V>>,
    pub freshness: ReadFreshness,
}

/// A `lookup` result: the row plus freshness, or `None` when absent.
#[derive(Clone, Debug)]
pub struct Lookup<K, V> {
    pub row: Option<Row<K, V>>,
    pub freshness: ReadFreshness,
}

/// Primary-key ordered page of decoded rows; `next_key` continues the
/// iteration, `None` at the end.
#[derive(Clone, Debug)]
pub struct IterateRows<K, V> {
    pub rows: Vec<Row<K, V>>,
    pub next_key: Option<K>,
    /// Journal position the page was pinned to.
    pub pinned_seq: u64,
    /// Node identity that served — diagnostics, never routing input.
    pub served_by: String,
}

/// A codec-bound declared StateStore. `Client::store` binds the default
/// utf8-key/bytes-payload codecs; `Client::store_with` binds explicit
/// codecs; `view` rebinds without a wire call.
#[derive(Clone)]
pub struct Store<K, V> {
    pub(crate) inner: StateStore,
    pub(crate) key_codec: Codec<K>,
    pub(crate) value_codec: Codec<V>,
}

impl<K, V> Store<K, V> {
    /// The declared store name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Byte-level access — the explicit escape hatch.
    pub fn raw(&self) -> &StateStore {
        &self.inner
    }

    /// Rebind codecs without a wire call — a view over the same store.
    pub fn view<KK, VV>(&self, key_codec: Codec<KK>, value_codec: Codec<VV>) -> Store<KK, VV> {
        Store {
            inner: self.inner.clone(),
            key_codec,
            value_codec,
        }
    }

    /// Put one row: codecs serialize key and payload; `mutation_id` is
    /// the idempotency identity (identical retries dedupe).
    pub async fn put(
        &self,
        mutation_id: Uuid,
        key: &K,
        value: &V,
    ) -> Result<MutationOutcome, SdkError> {
        self.put_named(mutation_id, key, value, &[]).await
    }

    /// Put one row plus named-key postings for `get_by` queries.
    pub async fn put_named(
        &self,
        mutation_id: Uuid,
        key: &K,
        value: &V,
        named: &[(String, Vec<TypedValue>)],
    ) -> Result<MutationOutcome, SdkError> {
        let key_bytes = self.key_codec.encode(key)?;
        let payload = self.value_codec.encode(value)?;
        self.inner
            .put_row(mutation_id, &key_bytes, &payload, named)
            .await
    }

    /// Delete one row under a `mutation_id` identity.
    pub async fn delete(&self, mutation_id: Uuid, key: &K) -> Result<MutationOutcome, SdkError> {
        let key_bytes = self.key_codec.encode(key)?;
        self.inner.delete_row(mutation_id, &key_bytes).await
    }

    /// Empty the store under a `mutation_id` identity — replay dedupes.
    pub async fn clear(&self, mutation_id: Uuid) -> Result<MutationOutcome, SdkError> {
        self.inner.clear(mutation_id).await
    }

    /// Point lookup decoding key and payload; freshness evidence comes
    /// through verbatim. `strict` bounded-waits for the replica's
    /// applied sequence to reach its committed snapshot.
    pub async fn get(&self, key: &K, strict: bool) -> Result<Lookup<K, V>, SdkError> {
        let key_bytes = self.key_codec.encode(key)?;
        let outcome = self.inner.lookup(&key_bytes, strict).await?;
        let row = match outcome.row {
            Some(row) => Some(Row {
                key: self.key_codec.decode(&row.key)?,
                value: self.value_codec.decode(&row.payload)?,
            }),
            None => None,
        };
        Ok(Lookup {
            row,
            freshness: outcome.freshness,
        })
    }

    /// Rows whose declared `named_key` posting equals one of `values`;
    /// decoded, with the same freshness contract as `get`.
    pub async fn get_by(
        &self,
        named_key: &str,
        values: &[TypedValue],
        limit: Option<u64>,
        strict: bool,
    ) -> Result<RowPage<K, V>, SdkError> {
        let page = self.inner.get_by(named_key, values, limit, strict).await?;
        Ok(RowPage {
            rows: self.decode_rows(&page.rows)?,
            freshness: page.freshness,
        })
    }

    /// The decoded value for `key`, `None` when absent — `get` without
    /// the row/freshness wrapper.
    pub async fn find(&self, key: &K) -> Result<Option<V>, SdkError> {
        Ok(self.get(key, false).await?.row.map(|row| row.value))
    }

    /// The first row whose declared `named_key` posting equals `value`,
    /// decoded — `get_by` for the fallback-lookup shape.
    pub async fn find_by(&self, named_key: &str, value: TypedValue) -> Result<Option<V>, SdkError> {
        let page = self.get_by(named_key, &[value], Some(1), false).await?;
        Ok(page.rows.into_iter().next().map(|row| row.value))
    }

    /// `find` with a caller fallback — counters and simple reads stay
    /// one line: `store.get_or_default(&key, 0).await? + 1`.
    pub async fn get_or_default(&self, key: &K, default: V) -> Result<V, SdkError> {
        Ok(self.find(key).await?.unwrap_or(default))
    }

    /// Primary-key ordered page of decoded rows; `after_key` continues
    /// from a prior page's `next_key`, `None` starts at the first row.
    /// The continuation key round-trips through the key codec so callers
    /// never touch its bytes.
    pub async fn iterate(
        &self,
        after_key: Option<&K>,
        limit: Option<u64>,
    ) -> Result<IterateRows<K, V>, SdkError> {
        let after = match after_key {
            Some(key) => Some(self.key_codec.encode(key)?),
            None => None,
        };
        let page = self.inner.iterate(after.as_deref(), limit).await?;
        let next_key = match &page.next_key {
            Some(bytes) => Some(self.key_codec.decode(bytes)?),
            None => None,
        };
        Ok(IterateRows {
            rows: self.decode_rows(&page.rows)?,
            next_key,
            pinned_seq: page.pinned_seq,
            served_by: page.served_by,
        })
    }

    /// Drive one catch-up pass on a feed-sourced store.
    pub async fn sync(&self) -> Result<SyncOutcome, SdkError> {
        self.inner.sync().await
    }

    /// Rebuild a feed-sourced store from retained history; `token` is
    /// the caller's retry identity for the pass.
    pub async fn rebuild(&self, token: Option<Uuid>) -> Result<SyncOutcome, SdkError> {
        self.inner.rebuild(token).await
    }

    fn decode_rows(&self, rows: &[crate::store::StoreRow]) -> Result<Vec<Row<K, V>>, SdkError> {
        rows.iter()
            .map(|row| {
                Ok(Row {
                    key: self.key_codec.decode(&row.key)?,
                    value: self.value_codec.decode(&row.payload)?,
                })
            })
            .collect()
    }
}
