# whitewater-rust

Rust SDK for Whitewater — typed application client for the versioned
`whitewater-protocol` wire contract defined in the main repository
under `sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).

- **Typed feeds and stores** — bind a `Codec` once; every append, fetch,
  get, and put serializes automatically.
- **Session-bound writers and readers** — epoch-fenced appends with
  caller-pinned `request_id` idempotency; delivered-cursor acks.
- **Subscription members** — join, claim, fetch, ack, renew, release.
- **Indexes, Pipes, StateStores** — typed index queries, forward/enrich/
  window Pipe defines plus describe/pause/resume/drop lifecycle, manual
  and feed-sourced stores with named-key extractors and freshness
  evidence.
- **Demand observability** — per-resource counters and per-domain
  rollups (`demand_feeds`, `demand_readers`, `demand_subscriptions`,
  `demand_indexes`, `demand_pipes`, `hot_keys`).

A complete microservice walkthrough — provision, produce, pump-process,
dead-letter, shutdown — lives in
[`docs/demo-service.md`](docs/demo-service.md).

## Creating a client

```rust
let client = Client::http(
    vec!["http://localhost:7371".to_owned()],       // Riverbed endpoints
    "whitewater-local-development-admin-key",       // API key
)?;
```

`client.health().await` checks liveness;
`client.check_compatible().await` verifies protocol compatibility. The
client also carries the flat administration surface — `wcl`/`commands`,
Domain and Role lifecycle — while every Feed, Writer, Reader,
Subscription, Member, Pipe, StateStore, and Index is reached **only**
through a `Domain`.

Whitewater's public model is `domain -> feed -> key -> cursor ->
subscription`: partitions, ranges, replicas, and physical positions are
internal implementation details — cursors are opaque tokens, never
parsed.

## Domains — the strongly-typed front door

A Domain is the administrative namespace every business resource lives
inside: a Feed named `shop.orders` belongs to the Domain `shop`, and
nothing outside a Domain can reach it. Writers, Readers, Subscriptions,
StateStores, Indexes, and Pipes resolve their relative names inside the
owning Domain, so a `"audit"` Reader declared on `shop` is
`shop.audit` everywhere it matters: progress, leases, WCL, demand
reports, and access control all scope to it. Cross-Domain references are
possible but always explicit (`Scoped::abs(...)` or a handle), which
makes accidental cross-wiring of two tenants' resources a deliberate
act rather than a silent mistake.

```rust
let shop = client.domain("shop")?;            // lazy handle — no I/O
let shop = client.get_domain("shop").await?;  // eager — verifies + loads
shop.create_feed("orders").await?;            // CREATE FEED shop.orders;

shop.feed_names().await?;                     // ['shop.orders', ...]
shop.feeds_prefixed("order").await?;          // prefix-filtered handles
shop.readers().await?; shop.writers().await?; // scoped catalog views
shop.subscriptions().await?; shop.pipes().await?;
shop.indexes().await?;       shop.state_stores().await?;
shop.describe().await?;      shop.domain_id();
shop.refresh().await?;                        // re-pull the cached snapshot

// Everything a service needs hangs off the Domain — relative names
// resolve inside it:
let writer = shop
    .create_writer(&orders)?                   // &Feed<K,V> keeps its codecs
    .writer_id("checkout")                     // durable producer identity — required
    .open()
    .await?;

let mut reader = shop
    .create_reader(&orders)?
    .reader_id("audit")                        // durable consume identity — Kafka groupId
    .reader_start_point(ReaderStart::Beginning) // first-declare only — auto.offset.reset
    .max_page_size(64)                          // max records per fetch
    .open()
    .await?;
```

`reader_id` is durable application state: `reader_start_point` applies
only when the Reader is first declared — an existing Reader resumes its
acknowledged frontier. `writer_id` is required because it's the
Writer's transactional identity: the fencing epoch binds to it and the
retry-dedup ledger lives under it. A `Feed` bound to another Domain is
refused locally, before any wire call. The first catalog accessor
materializes one batched control read — `DESCRIBE DOMAIN` plus every
`SHOW` listing — and caches it; `domain.refresh().await?` re-pulls.

### Record schemas

Records stay schema-neutral bytes on the wire; a Feed can still declare
schema metadata the catalog tracks for tooling, validation, and
compatibility. The Domain's schema is the default every Feed inherits;
a Feed's own declaration overrides the named sides:

```rust
client.set_domain_schema("shop", &FeedSchema {
    value: Some(SchemaDecl { format: "json".into(), subject: Some("order-v1".into()), definition: None }),
    ..FeedSchema::default()
}).await?;
shop.set_feed_schema("orders", &FeedSchema {
    key: Some(SchemaDecl { format: "text".into(), subject: None, definition: None }),
    ..FeedSchema::default()
}).await?;

shop.schema().await?;                      // the Domain default
shop.feed_schema("orders").await?;         // effective: key=text, value=json/order-v1
shop.declared_feed_schema("orders").await?; // the Feed's own sides only
shop.clear_feed_schema("orders").await?;   // reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments. Formats today: `json`, `jsonschema`, `avro`, `protobuf`,
`text`, `bytes`, or `custom.<name>` — compatibility enforcement is
still a no-op.

## Working with Feeds

`shop.feed_with("orders", Codec::utf8(), Codec::<Order>::json())` binds
a feed to key and payload codecs once; every write encodes and every
read decodes through them — application code never serializes per
record. `Codec<T>` is a bidirectional value contract — `encode`/`decode`
once, every operation uses it. Built-ins: `Codec::utf8()` (UTF-8
`String`), `Codec::bytes()` (`Vec<u8>` unchanged), `Codec::<T>::json()`
(JSON for any `Serialize + DeserializeOwned`), and
`Codec::custom(name, enc, dec)` (protobuf, Avro, MessagePack, framing).

`domain.feed(name)` binds the cross-SDK defaults (utf8 keys, raw byte
payloads); `feed.view(kc, vc)`/`store.view(kc, vc)` rebind without a
wire call. Codec failures surface as `SdkError::Codec` — typed and
non-retryable, since an unserializable value is never ambiguous
server-side.

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct Order { id: String, total: i64 }

let orders = shop.feed_with("orders", Codec::utf8(), Codec::<Order>::json())?;
```

### Reading — durable Readers and ad-hoc fetches

A **Reader** owns a durable, server-side cursor — the progress identity
that survives restarts, exactly like a committed Kafka group offset but
without the group protocol:

```rust
let mut reader = shop
    .create_reader(&orders)?
    .reader_id("audit")                         // durable consume identity
    .reader_start_point(ReaderStart::Beginning) // first-declare start position
    .max_page_size(64)                          // max records per fetch
    .open()
    .await?;

let page = reader.fetch(16).await?;             // up to max_page_size records
for message in &page.records {
    println!("{:?} -> {:?}", message.key, message.value.id);
}
reader.ack().await?;                            // commits the delivered frontier
reader.close().await?;                          // releases the session fence
```

`reader.for_each(handler)` runs the same fetch→ack cycle for you —
deliver each record, then ack each handled page; `Ok(false)` or an
error stops with the page unacked:

```rust
reader.for_each(|m| async move {
    handle(&m).await?;
    Ok(true)                                // false stops, page unacked
}).await?;
```

For ad-hoc reads without a durable identity, `feed.records(limit)`,
`feed.fetch`/`domain.temporary_fetch` (`tail`/`wait_ms` long-poll,
`after_event_time_ns` seeks, `new_only`), and `feed.load(start, sink)`
one-shot replays cover committed history.

### Shared progress — subscriptions and members

A named **Subscription** is the durable shared frontier cooperating
readers claim work against — the Kafka consumer-group model, minus the
rebalance storm. `domain.member(subscription)` joins a member and runs
the work loop:

```rust
let member = shop.member("orders-sub").await?;
member.renew().await?;                        // keep the lease alive
let work = member.claim(None).await?;         // RF3-fenced work lease
let page = member.fetch(&work, 256).await?;   // records for this member
member.ack(&work, &page).await?;              // commits shared progress
member.release(&work).await?;                 // or transfer to another member
```

Leases are fenced — a lost or expired claim is refused, and work
transfers cleanly to another member. `Feed::subscribe` runs the managed
deliver→ack loop for you.

### The read loop — consumers

`feed.consumer(name)` binds the durable named Reader (the
consumer-group equivalent) and owns the fetch → deliver → ack cycle:
session re-opens, transient backoff, and graceful drain are all
internal. Application code only sees decoded records, one at a time:

```rust
let mut consumer = feed.consumer("enricher");
while let Some(message) = consumer.next().await? {
    process(&message).await;      // Err -> the page stays unacked and
}                                 //         redelivers (at-least-once)

// Or a handler form completing only on close/stop:
let stop = consumer.handle();      // call stop() from a signal task
feed.consumer("enricher")
    .start_at(ReaderStart::Beginning)          // first declaration only
    .backoff(Duration::from_millis(250), Duration::from_secs(30))
    .on_error(|e| eprintln!("consumer restarted: {e}"))
    .run(|msg| async move { process(msg).await?; Ok(()) })
    .await?;
```

- The named Reader's acknowledged cursor is durable server-side — a
  service restart resumes where it stopped; no offset management.
- Each fetched page is delivered record-by-record; its delivered
  cursor acks only after every record delivered cleanly. A handler
  error (or an abandoned consumer) leaves the page unacked, so it
  redelivers — route poison pills to a DLQ feed yourself.
- Transient failures (`Unavailable`, `Ambiguous`, `Transport`) retry
  with exponential backoff; a fenced session (`StaleEpoch`,
  `Conflict`) re-opens and resumes. `Refused` and local codec/protocol
  faults surface on `next`/`run` — fail fast, never loop.
- `consumer.close().await` (or `handle.stop()`) finishes the
  in-flight page, acks it, releases the session — the SIGTERM path.

See [examples/enrich_users.rs](examples/enrich_users.rs) for the full
enrichment service — primary/named-key store lookups, DLQ, counters.

### Writing

```rust
// Declares shop.checkout and resolves each append after majority
// commit (at least 2 of 3 Nodes):
let writer = shop
    .create_writer(&orders)?
    .writer_id("checkout")                     // durable producer identity — required
    .open()
    .await?;
let appended = writer
    .append(&"order-42".to_owned(), &Order { id: "o-42".into(), total: 99 })
    .await?;
// appended: message_id, cursor, deduplicated, durability — the ack payload

// Idempotent events: pin request_id for caller-driven retries —
// the same identity replays its committed result instead of writing twice.
writer.append_event(&FeedEvent::new("order-42", &order)
    .with_request_id("a-stable-request-uuid")
    .with_event_time_ns(event_time_ns)
    .with_metadata(metadata)).await?;

// One commit for a group of records:
writer.append_batch(&[e1, e2]).await?;
```

Every append carries an idempotent `request_id` — retrying a timed-out
append with the same identity deduplicates; reusing it with different
contents is refused. Writers are epoch-fenced sessions: a second writer
claiming `checkout` fences the first (`StaleEpoch` → `reopen()`), so an
ambiguous timeout can never double-commit under retry.
`use_compression` opts into LZ4; record `event_time_ns`/
`ingest_time_ns` are `i64` nanoseconds serialized as decimal strings on
the wire.

## Working with StateStores

StateStores are durable, replicated key/value views that live on the
Nodes — the SDK holds no local copy. Declare named keys up front and
each becomes a maintained secondary index.

### Declaring a store — named keys

```rust
let spec = StateStoreSpec::new("employees")
    .add_unique_key("passport")            // one row per value
    .add_non_unique_key("department");     // value → many rows; up to MAX_NAMED_KEYS
let store = shop.define_state_store(spec, None).await?;  // None = manual store
```

**Unique** (`passport`, `email`, `order_number`) — one value maps to at
most one row; `get_by` returns the single match and a second claim on
the same value arbitrates first-committed-wins. **Non-unique**
(`department`, `status`, `region`) — one value maps to many rows;
`get_by` returns every match as a bounded page.

Two store kinds: **manual** (your app puts rows — `None` source) and
**feed-sourced** (the Nodes replay a source Feed and project rows +
postings via `add_unique_key_with`/`add_non_unique_key_with` +
`Extractor` — required on feed-sourced keys, refused on manual ones).

### Putting and deleting

Named-key postings take a typed key-set builder or a raw map:

```rust
let profiles = shop.store_with("employees", Codec::utf8(), Codec::<Profile>::json())?;

profiles.put_with_key("emp-1", &profile,
    PassportKeySet::new().passport("P123").department("eng")).await?; // typed key set
profiles.put_named("emp-1", &profile, named_map).await?;             // raw map — no checking

store.delete("emp-1").await?;        // mutation id generated
store.clear().await?;                // one journaled ClearAll
```

Every mutation carries an idempotent `mutation_id` — generated per
call, or pinned for caller-driven retries.

### Reading

```rust
let row = store.get("emp-1").await?;              // StoreRow + freshness evidence
let profile = profiles.find(&email).await?        // value only — skips the row wrapper
    .or(profiles.find_by("user_id", TypedValue::Text(uid)).await?);
let n = counters.get_or_default(&bucket, 0).await?;   // counters, one line

let rows = store.get_by("department", TypedValue::Text("eng".into())).await?; // every match
let mut it = store.iterate();                     // primary-key order
while let Some(r) = it.next().await? { export(r) }
```

`get`/`get_by` return rows carrying applied-cursor freshness evidence —
strict reads bounded-wait for the replica to apply its committed
frontier rather than answering stale. The byte-level
`domain.state_store(name)`/`state_store_raw(name)` facades remain for
raw access (`put_row`/`delete_row`/`lookup`/`iterate`). See
[docs/stores.md](docs/stores.md) for the full store guide.

## Working with Pipes

Managed Pipes are server-run stream jobs: you declare the business
shape — source Subscription, output Feed, operation — and the server
owns the subscription frontier, compute lease, panes, and the effect
journal. Every endpoint is a `Scoped`: `Scoped::rel` resolves
in-domain; `Scoped::abs` or a `Feed`/`Store`/`SubscriptionRef` handle
crosses Domains explicitly. The Pipe's own name and its input
Subscription always stay in the declaring Domain. See
[docs/pipes.md](docs/pipes.md) for the full pipe guide.

### Simple — forwarding

```rust
shop.define_forward_pipe("relay",
    Scoped::rel("orders-sub"),    // source Subscription
    Scoped::rel("relayed")).await?; // output Feed, same Domain
```

### Enrichment — StateStore lookups

```rust
shop.define_enrich_pipe("enrich",
    Scoped::rel("orders-sub"), Scoped::rel("enriched"),
    Scoped::abs("crm.customers"),             // cross-Domain lookup store
    PipeOperation::enrich(Extractor::record_key(),  // join on record key
        EnrichOptions { freshness: EnrichFreshness::Latest,
            on_miss: EnrichMiss::Quarantine, ..Default::default() }),
    Some(Scoped::rel("enrich-dlq"))).await?;
```

### Time-based — windowing

```rust
shop.define_window_pipe("hourly",
    Scoped::rel("orders-sub"), Scoped::abs("stats.orders-hourly"),
    PipeOperation::window(Duration::from_secs(3600),  // 1h panes
        Extractor::record_key(), WindowAggregation::count(),
        WindowOptions { time: WindowTime::Event,
            watermark: Duration::from_secs(300),  // 5m horizon
            grace: Duration::from_secs(30),       // 30s late revisions
            expire: Some(Duration::from_secs(7200)), ..Default::default() }),
    Some(Scoped::rel("hourly-emitted"))).await?;
```

### Indexes and observability

```rust
// Pipe lifecycle: describe, pause, resume, drop.
let pipe = shop.pipe("relay")?;
let info = pipe.describe().await?;    // stage, operation, outputs, status
pipe.pause().await?; pipe.resume().await?; pipe.drop().await?;
let pipes = shop.pipe_infos().await?; // the declared catalog, in-domain

// Index queries: exact, prefix, or range predicates over typed values.
let page = crm.query_index("byregion",
    IndexQuery::range(
        vec![TypedValue::Text("emea".into())],
        IndexBound::inclusive(TypedValue::I64(0)),
        IndexBound::exclusive(TypedValue::I64(99))),
    50,        // limit
    true).await?; // strict: wait for the committed target rather than stale
for row in page.rows { /* row.key / row.payload bytes */ }

// Demand observability: typed rows + per-domain rollups.
let report = client.demand_feeds().await?;
let appended = report.rows[0].counter("appended_records");
client.demand_readers().await?; client.demand_subscriptions().await?;
client.demand_indexes().await?;  client.demand_pipes().await?;
let hot = client.hot_keys().await?;    // dominant-key evidence per feed
```

Pipe operation payloads and demand counter fields (`DemandRow` fields,
`HotKey` evidence) stay verbatim maps so server-side variants evolve
without an SDK bump. `client.hot_keys()` reports dominant-Key evidence —
the operator signal to spread or shed load, since a strictly ordered
hot Key is never parallelized.

## Errors, cursors, and time

`SdkError` maps wire `(status, code)` to typed variants:
`StaleEpoch` (reopen the session), `LeaseBusy`/`LeaseStale`/
`LeaseCapacity` (subscription work fencing), `IndexBehind` (strict
freshness unreachable in the bounded wait), `Unavailable`/`Ambiguous`/
`Transport` (`retryable()` — retry the identical request), `Conflict`,
`Refused`, `IncompatibleServer`, `Protocol`, `Codec`.

Cursors are opaque: the SDK never parses, orders, or constructs them.
`event_time_ns`/`ingest_time_ns` are `i64` epoch nanoseconds traveling
as decimal strings on the wire; `Duration` covers window and delay
declarations.

## Live tests

Unit tests run against a scripted `Transport` — no Node needed.
`WHITEWATER_ENDPOINTS`/`WHITEWATER_API_KEY` gate live checks; the
conformance suite (`sdk/spec/conformance` in the Whitewater repository)
drives the same `Client::wire` escape hatch.

## Layout

```
src/
  client.rs       endpoint pool, health/limits, WCL/commands, wire
  writer.rs       session-bound Writer
  reader.rs       session-bound Reader
  member.rs       Subscription member (claim/fetch/ack/renew/release)
  store.rs        byte-level StateStore + freshness/outcome types + StateStoreSpec
  pipe.rs         declared Pipe lifecycle
  operations.rs   PipeOperation / WindowOperation / WindowAggregation wire shapes
  typed.rs        TypedValue + IndexQuery/IndexPage + Extractor
  demand.rs       demand counters + hot-Key evidence
  codec.rs        Codec<T> — utf8 / bytes / json / custom
  feed.rs         Feed<K,V>, FeedWriter, FeedReader, subscribe
  consumer.rs     FeedConsumer — supervised next()/run() + ReaderStart
  typed_store.rs  Store<K,V>
  error.rs        SdkError + wire status/code mapping
  transport.rs    Transport trait + HttpTransport (reqwest/rustls)
  model.rs        Record/AppendOutcome/pages + wire decoders
examples/
  typed_feed.rs   end-to-end typed write/read against a dev Node
  enrich_users.rs permanent consumer: store lookup, DLQ, counters
```

## Full API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`commands`, `wcl`, and the byte-level `Writer`/`Reader`/`StateStore`/
`Member` types are the explicit escape hatches for the same wire
contract.

| Method | Parameters | Description |
|---|---|---|
| **Client & transport** | | |
| `Client::http(endpoints, api_key)` `Client::new` | endpoints, api key | Client construction |
| `health` `check_compatible` `transport_limits` `use_compression` `with_api_key` `wire` | — | Liveness, compatibility, LZ4, wire access |
| **Domains — the only door** | | |
| `create_domain(name)` / `domain(name)` / `get_domain(name)` | relative name | Declare / lazy / eager handle |
| `feed(name)` `feed_with(name, Codec, Codec)` `get_feed` `store` `store_with` `view` | name, `Codec<T>` | Typed `Feed<K,V>`/`Store<K,V>` handles |
| `create_feed` `create_reader` `create_writer` `create_subscription` | relative name or `Feed` handle | Declare + open inside the Domain |
| `feeds` `feeds_prefixed` `feed_names` `readers` `writers` `subscriptions` `pipes` `indexes` `state_stores` | optional prefix | Domain catalog views |
| `name` `domain_id` `describe` `refresh` | — | Domain metadata |
| `set_domain_schema` `clear_domain_schema` `set_feed_schema` `clear_feed_schema` `schema` `feed_schema` `declared_feed_schema` | `FeedSchema`/`SchemaDecl` — `json`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` | Record-schema declarations |
| **Feeds** | | |
| `records` `fetch` `load(start, sink)` | limit, cursor, `ReaderStart` | Committed history reads |
| `writer` `reader` `consumer` `subscribe` | name, options | Bound sessions + loops |
| **Writing** | | |
| `FeedWriter::append` `append_event` `append_batch` | key, value, `FeedEvent` | Write resolving on majority commit — `AppendOutcome` |
| `reopen` `session_epoch` `use_compression` | — | Session + lifecycle |
| **Reading** | | |
| `FeedReader::fetch` `ack` `close` `session_epoch` `acknowledged_cursor` | page bound, cursor | Manual durable-Reader loop |
| `for_each(handler)` | record handler | SDK-owned read pump |
| `ReaderStart` | `Beginning` `Now` `Cursor` `TimestampNs` | First-declare start position |
| `temporary_fetch` `Feed::records` `Feed::load` | `after_event_time_ns`, `new_only`, `tail`, `wait_ms` | Ad-hoc + replay reads |
| `feed.consumer(name)` → `FeedConsumer`/`ConsumerHandle` | `start_at` `capacity` `backoff` `on_error` | Supervised durable consumer: `next`/`run`/`stop`/`wait`/`close`/`handle` |
| **Subscription members** | | |
| `domain.member(subscription)` → `Member` | `member_id` `member_epoch` `claim` `fetch` `ack` `renew` `release` | Shared progress + RF3 work leases |
| `Feed::subscribe` | reader, feed, handler | Managed pump loop |
| **StateStores** | | |
| `StateStoreSpec::new` `define_state_store` `drop_state_store` `store` `store_with` `state_store` `state_store_raw` | spec, source Feed, codecs | Store lifecycle + typed/byte-level handles |
| `put` `put_named` `put_keys` `put_with_key` `delete` `clear` | key, value, named-key postings | Mutations |
| `get` `get_by` `find` `find_by` `get_or_default` `iterate` `lookup` | key, named key, bounds | Row + named-key reads |
| `add_unique_key` `add_non_unique_key` `*_with(Extractor)` `named_keys` `sync` `rebuild` `view` `raw` | key name, `Extractor` | Spec + administration (`MAX_NAMED_KEYS`) |
| **Indexes** | | |
| `query_index(name, query, limit, strict)` | `IndexQuery`/`IndexBound`/`TypedValue` | Decoded index rows |
| **Pipes** | | |
| `define_forward_pipe` `define_enrich_pipe` `define_window_pipe` | name, `Scoped` (`rel`/`abs`/handles), `PipeOperation`/`EnrichOptions`/`WindowOptions` | Managed jobs (`Duration` throughout) |
| `pipe(name)` `pipe_infos()` | — | `describe` `pause` `resume` `drop` |
| **Observability** | | |
| `demand_feeds` `demand_readers` `demand_subscriptions` `demand_indexes` `demand_pipes` `hot_keys` | optional feed | `DemandReport`/`DemandRow::counter` + `HotKey` evidence |
| **Escape hatches & errors** | | |
| `wcl(script)` `commands(Vec<Value>)` `Writer`/`Reader`/`Member`/`StateStore` | script, raw commands | Byte-level wire access |
| `SdkError` | `retryable()` | `Refused` `StaleEpoch` `Sequence` `LeaseBusy` `LeaseStale` `LeaseCapacity` `Conflict` `Ambiguous` `Unavailable` `IndexBehind` `IncompatibleServer` `Protocol` `Codec` |
| `Transport` trait | scripted stub | Full-stack test seam |
