# StateStore — coding guide

There is exactly one store: the **node-side StateStore** — durable,
RF3-replicated, Fjall-backed, and queryable from any Node. The SDK's
`StateStore` is a thin facade over it: it holds no rows, keeps no local
projection, and every operation is an HTTP call with freshness evidence.

```rust
use whitewater::{Client, StateStoreSpec, Extractor};

let accounts = client.create_domain("accounts").await?;
let spec = StateStoreSpec::new("employees")?           // -> accounts.employees
    .add_unique_key("passport", None)?      // unique: one row per value
    .add_non_unique_key("department", None)?;  // non-unique: value -> many rows

let store = accounts.define_state_store(spec, None).await?;   // manual store
```

`add_unique_key`/`add_non_unique_key` record declaration intent used by `define_state_store` — named
keys are declared at creation; at most `MAX_NAMED_KEYS` (10), unique or
not. Adding a key to an existing store has no effect server-side.

## Feed-sourced stores

Pass `source: Some(feed)` and the Nodes materialize rows from that Feed's
history — every named key must declare an `Extractor` projecting each
source record to the key's values:

```rust
let spec = StateStoreSpec::new("customers")?           // -> ops.customers
    .add_unique_key("email", Some(Extractor::json("$.email")?))?
    .add_non_unique_key("tier", Some(Extractor::json("$.tier")?))?;
ops.define_state_store(spec, Some("customer.changes".into())).await?;
```

Extractors project a committed record to a typed field value:

```rust
Extractor::record_key()          // the record's application Key
Extractor::metadata("tenant")?   // a named record Metadata entry
Extractor::json("$.profile.id")? // a dotted JSON-path over the payload
```

A manual store refuses extractors (values arrive on each `put_row`);
a feed-sourced store refuses named keys without them.

## Writing & `StoreKeySet`

Wrap `StoreKeyMap` in a typed struct implementing `StoreKeySet` (or use `StoreKeyMap::new()` / `put_with_key` directly):

```rust
use whitewater::{StoreKeyMap, StoreKeySet, TypedValue};
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct PassportKeySet(StoreKeyMap);

impl PassportKeySet {
    pub fn passport(mut self, v: impl Into<String>) -> Self {
        self.0.set("passport", v.into());
        self
    }
    pub fn department(mut self, v: impl Into<String>) -> Self {
        self.0.set("department", v.into());
        self
    }
    pub fn with(passport: impl Into<String>, department: impl Into<String>) -> Self {
        Self::default().passport(passport).department(department)
    }
    pub fn with_passport(passport: impl Into<String>) -> Self {
        Self::default().passport(passport)
    }
}

impl StoreKeySet for PassportKeySet {
    fn to_named_postings(&self) -> Vec<(String, Vec<TypedValue>)> {
        self.0.to_named_postings()
    }
}

let typed_store = accounts.store("employees")?;
typed_store.put_keys(Uuid::new_v4(), &"emp-1".into(), &profile, &PassportKeySet::with("P123", "eng")).await?;
typed_store.put_keys(Uuid::new_v4(), &"emp-2".into(), &profile, &PassportKeySet::with_passport("P222").department("ops")).await?;
typed_store.put_with_key(Uuid::new_v4(), &"emp-3".into(), &profile, "P333").await?;
```

Or on the byte-level `StateStore`:

```rust
use whitewater::TypedValue;
use uuid::Uuid;

store
    .put_row(
        Uuid::new_v4(),                    // mutation_id: idempotency identity
        b"user-1",
        b"profile-1",
        &[
            ("passport".to_owned(), vec![TypedValue::Text("P111".to_owned())]),
            ("department".to_owned(), vec![TypedValue::Text("eng".to_owned())]),
        ],
    )
    .await?;
store.delete_row(Uuid::new_v4(), b"user-1").await?;
store.clear(Uuid::new_v4()).await?;
```

- `put_row` commits one journaled `put_row` through the store's RF3
  quorum. `key`/`payload` are raw bytes — the typed `Store<K, V>` facade
  (below) encodes through codecs.
- `named` values are **typed** `TypedValue`s: `Text`, `I64`, `Bool`,
  `Bytes`, `Null`. A `Vec` carries one typed value per posting component.
- Named values must match the store's declared keys — the server refuses
  undeclared names. `unique` claims arbitrate first-committed-wins by
  journal order.
- `mutation_id` is the idempotency identity: retry a timed-out put with
  the same id and it dedupes; reuse it with different contents and it
  conflicts.

## Reading

```rust
let outcome = store.lookup(b"user-1", false).await?;
if let Some(row) = outcome.row {
    // row.key / row.payload are bytes
}
// outcome.freshness: applied_seq / committed_seq / behind / served_by

let page = store
    .get_by("department", &[TypedValue::Text("eng".to_owned())], Some(100), false)
    .await?;
// page.rows — every matching (key, payload) pair
// page.freshness.served_by — which Node answered
```

`get_by(named_key, values, limit, strict)` is the group/list lookup: one
non-unique named value resolves to **every** primary row carrying it.
Pass `strict = true` on `lookup`/`get_by` and the serving replica
bounded-waits for its applied journal to reach the committed frontier
rather than answering stale; non-strict reads still report
`freshness.behind` honestly.

## Iterating

```rust
let page = store.iterate(None, Some(256)).await?;
for row in &page.rows { /* primary-key order */ }
let next = page.next_key.as_deref();    // continuation, or None at end
// page.pinned_seq — journal position the page was pinned to
let page2 = store.iterate(next, Some(256)).await?;
```

`iterate` pages `/v1/state-stores/iterate` — the same bounded, ordered
row set replica catch-up serves — so `pinned_seq` is the iteration's
freshness evidence.

## Typed facade

`domain.store::<K, V>(name)` binds `Codec`s so keys/values decode at the
boundary — `put`/`get`/`find`/`find_by`/`get_or_default` are the
one-value conveniences over `put_row`/`lookup`/`get_by`:

```rust
let typed = accounts.store::<String, String>("employees")?;
typed
    .put_named(
        Uuid::new_v4(),
        &"user-1".to_owned(),
        &"profile-1".to_owned(),
        &[("department".to_owned(), vec![TypedValue::Text("eng".to_owned())])],
    )
    .await?;
let row = typed.get(&"user-1".to_owned(), false).await?;
let hits = typed
    .get_by("department", &[TypedValue::Text("eng".to_owned())], None, false)
    .await?;
let iter = typed.iterate(None, Some(256)).await?;   // decoded keys too
```

`store.view(other_key_codec, other_value_codec)` rebinds the same store
under different codecs — no wire call.

## Feed-sourced maintenance

```rust
store.sync().await?;                 // one catch-up pass
store.rebuild(None).await?;          // replay retained history from zero
                                     // (token = retry identity)
```

## Dropping

```rust
client.wcl("DROP STATESTORE accounts.employees;").await?;
```

## Freshness evidence

Every read returns `ReadFreshness` — the serving replica's
`applied_seq`/`committed_seq`, whether it still lags (`behind`), and
`served_by` (the Node identity — diagnostics, never routing input).
A `strict` read that cannot reach the committed frontier answers
`SdkError::IndexBehind`, a retryable typed error.
