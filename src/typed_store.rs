//! Codec-bound typed StateStores: `Store<K, V>` serializes keys and
//! row payloads through bound codecs on every get/put/iterate — the
//! same convenience contract as typed Feeds.

use uuid::Uuid;

use crate::codec::Codec;
use crate::error::SdkError;
use crate::store::{
    MutationOutcome, MutationScope, ReadFreshness, ReadOpts, StateStore, StoreKeySet, SyncOutcome,
};
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

    /// Put one row: codecs serialize key and payload; the mutation id
    /// generates per call — [`Self::with_mutation_id`] binds a stable
    /// idempotency identity for retry-safe sequences.
    pub async fn put(&self, key: &K, value: &V) -> Result<MutationOutcome, SdkError> {
        self.put_named(key, value, &[]).await
    }

    /// Bind the default secondary-key name used by [`Self::put_with_key`].
    pub fn with_default_key(mut self, default_key: impl Into<String>) -> Self {
        self.inner = self.inner.with_default_key(default_key);
        self
    }

    /// Scope mutations under a caller-supplied idempotency identity —
    /// `store.with_mutation_id(id).put(&key, &value)`.
    pub fn with_mutation_id(&self, mutation_id: Uuid) -> MutationScope<'_, Store<K, V>> {
        MutationScope {
            store: self,
            mutation_id,
        }
    }

    /// Put one row plus a typed [`StoreKeySet`].
    pub async fn put_keys(
        &self,
        key: &K,
        value: &V,
        keys: &impl StoreKeySet,
    ) -> Result<MutationOutcome, SdkError> {
        let named = keys.to_named_postings();
        self.put_named(key, value, &named).await
    }

    /// Put one row with a single default secondary-key string (mapped to the store's first declared key).
    pub async fn put_with_key(
        &self,
        key: &K,
        value: &V,
        single_secondary_key: impl Into<String>,
    ) -> Result<MutationOutcome, SdkError> {
        let named = vec![(
            self.inner.resolve_default_key_name(),
            vec![TypedValue::Text(single_secondary_key.into())],
        )];
        self.put_named(key, value, &named).await
    }

    /// Put one row plus named-key postings for `get_by` queries.
    pub async fn put_named(
        &self,
        key: &K,
        value: &V,
        named: &[(String, Vec<TypedValue>)],
    ) -> Result<MutationOutcome, SdkError> {
        let key_bytes = self.key_codec.encode(key)?;
        let payload = self.value_codec.encode(value)?;
        self.inner.put_row(&key_bytes, &payload, named).await
    }

    /// Delete one row; the mutation id generates per call —
    /// [`Self::with_mutation_id`] binds a stable one.
    pub async fn delete(&self, key: &K) -> Result<MutationOutcome, SdkError> {
        let key_bytes = self.key_codec.encode(key)?;
        self.inner.delete_row(&key_bytes).await
    }

    /// Empty the store; the mutation id generates per call.
    pub async fn clear(&self) -> Result<MutationOutcome, SdkError> {
        self.inner.clear().await
    }

    /// Point lookup decoding key and payload; freshness evidence comes
    /// through verbatim — non-strict. [`Self::get_strict`] bounded-waits
    /// for the replica's committed snapshot, [`Self::get_opts`] is the
    /// full primitive.
    pub async fn get(&self, key: &K) -> Result<Lookup<K, V>, SdkError> {
        self.get_opts(key, &ReadOpts::new()).await
    }

    /// Point lookup that bounded-waits for the serving replica to apply
    /// its committed frontier rather than answering stale.
    pub async fn get_strict(&self, key: &K) -> Result<Lookup<K, V>, SdkError> {
        self.get_opts(key, &ReadOpts::new().strict()).await
    }

    /// The full point-lookup primitive — [`ReadOpts::strict`] or a
    /// bounded [`ReadOpts::wait`].
    pub async fn get_opts(&self, key: &K, opts: &ReadOpts) -> Result<Lookup<K, V>, SdkError> {
        let key_bytes = self.key_codec.encode(key)?;
        let outcome = self.inner.lookup_opts(&key_bytes, opts).await?;
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
    /// [`Self::get_by_opts`] adds a row `limit` and strict freshness.
    pub async fn get_by(
        &self,
        named_key: &str,
        values: &[TypedValue],
    ) -> Result<RowPage<K, V>, SdkError> {
        self.get_by_opts(named_key, values, &ReadOpts::new()).await
    }

    /// The full `get_by` primitive — [`ReadOpts::limit`] caps the page,
    /// [`ReadOpts::strict`]/[`ReadOpts::wait`] bound freshness.
    pub async fn get_by_opts(
        &self,
        named_key: &str,
        values: &[TypedValue],
        opts: &ReadOpts,
    ) -> Result<RowPage<K, V>, SdkError> {
        let page = self.inner.get_by_opts(named_key, values, opts).await?;
        Ok(RowPage {
            rows: self.decode_rows(&page.rows)?,
            freshness: page.freshness,
        })
    }

    /// The decoded value for `key`, `None` when absent — `get` without
    /// the row/freshness wrapper.
    pub async fn find(&self, key: &K) -> Result<Option<V>, SdkError> {
        Ok(self.get(key).await?.row.map(|row| row.value))
    }

    /// The first row whose declared `named_key` posting equals `value`,
    /// decoded — `get_by` for the fallback-lookup shape.
    pub async fn find_by(&self, named_key: &str, value: TypedValue) -> Result<Option<V>, SdkError> {
        let page = self
            .get_by_opts(named_key, &[value], &ReadOpts::new().limit(1))
            .await?;
        Ok(page.rows.into_iter().next().map(|row| row.value))
    }

    /// `find` with a caller fallback — counters and simple reads stay
    /// one line: `store.get_or_default(&key, 0).await? + 1`.
    pub async fn get_or_default(&self, key: &K, default: V) -> Result<V, SdkError> {
        Ok(self.find(key).await?.unwrap_or(default))
    }

    /// First primary-key ordered page of decoded rows;
    /// [`Self::iterate_from`] continues a prior page's `next_key`,
    /// [`Self::iterate_opts`] adds `limit`. The continuation key
    /// round-trips through the key codec so callers never touch bytes.
    pub async fn iterate(&self) -> Result<IterateRows<K, V>, SdkError> {
        self.iterate_opts(None, &ReadOpts::new()).await
    }

    /// Continue iteration from a prior page's `next_key`.
    pub async fn iterate_from(&self, after_key: &K) -> Result<IterateRows<K, V>, SdkError> {
        self.iterate_opts(Some(after_key), &ReadOpts::new()).await
    }

    /// The full iterate primitive — `after_key` resumes a page,
    /// [`ReadOpts::limit`] bounds it.
    pub async fn iterate_opts(
        &self,
        after_key: Option<&K>,
        opts: &ReadOpts,
    ) -> Result<IterateRows<K, V>, SdkError> {
        let after = match after_key {
            Some(key) => Some(self.key_codec.encode(key)?),
            None => None,
        };
        let page = self
            .inner
            .iterate_opts(&ReadOpts {
                after,
                ..opts.clone()
            })
            .await?;
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

    /// Rebuild a feed-sourced store from retained history.
    /// [`Self::rebuild_with`] carries a caller retry token for the pass.
    pub async fn rebuild(&self) -> Result<SyncOutcome, SdkError> {
        self.inner.rebuild().await
    }

    /// Rebuild with an explicit retry `token` — the same token marks a
    /// resumed pass.
    pub async fn rebuild_with(&self, token: Uuid) -> Result<SyncOutcome, SdkError> {
        self.inner.rebuild_with(token).await
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

impl<K, V> MutationScope<'_, Store<K, V>> {
    /// Put one row under the scope's idempotency identity.
    pub async fn put(&self, key: &K, value: &V) -> Result<MutationOutcome, SdkError> {
        self.put_named(key, value, &[]).await
    }

    /// Put one row plus a typed [`StoreKeySet`] under the scope's
    /// idempotency identity.
    pub async fn put_keys(
        &self,
        key: &K,
        value: &V,
        keys: &impl StoreKeySet,
    ) -> Result<MutationOutcome, SdkError> {
        let named = keys.to_named_postings();
        self.put_named(key, value, &named).await
    }

    /// Put one row with a single default secondary-key string under the
    /// scope's idempotency identity.
    pub async fn put_with_key(
        &self,
        key: &K,
        value: &V,
        single_secondary_key: impl Into<String>,
    ) -> Result<MutationOutcome, SdkError> {
        let named = vec![(
            self.store.inner.resolve_default_key_name(),
            vec![TypedValue::Text(single_secondary_key.into())],
        )];
        self.put_named(key, value, &named).await
    }

    /// Put one row plus named-key postings under the scope's
    /// idempotency identity.
    pub async fn put_named(
        &self,
        key: &K,
        value: &V,
        named: &[(String, Vec<TypedValue>)],
    ) -> Result<MutationOutcome, SdkError> {
        let key_bytes = self.store.key_codec.encode(key)?;
        let payload = self.store.value_codec.encode(value)?;
        self.store
            .inner
            .put_row_with_id(self.mutation_id, &key_bytes, &payload, named)
            .await
    }

    /// Delete one row under the scope's idempotency identity.
    pub async fn delete(&self, key: &K) -> Result<MutationOutcome, SdkError> {
        let key_bytes = self.store.key_codec.encode(key)?;
        self.store
            .inner
            .delete_row_with_id(self.mutation_id, &key_bytes)
            .await
    }

    /// Empty the store under the scope's idempotency identity.
    pub async fn clear(&self) -> Result<MutationOutcome, SdkError> {
        self.store.inner.clear_with_id(self.mutation_id).await
    }
}
