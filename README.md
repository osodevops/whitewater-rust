# whitewater-rust

Rust SDK for Whitewater — typed application client for the wire
protocol (`sdk/spec/PROTOCOL.md`, `sdk/spec/endpoints.json`). The public
model is `riverbed -> domain -> feed -> key -> cursor -> subscription`:
no partition, range, owner, or replica concept crosses the client
boundary, and Cursors are opaque.

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

A complete microservice walkthrough — provision, produce, pump-process, dead-letter, shutdown — lives in [`docs/demo-service.md`](docs/demo-service.md).

## Quick start

```rust
use whitewater::{Client, Codec, FeedEvent, ReaderStart, TemporaryFetch};

#[derive(serde::Serialize, serde::Deserialize)]
struct Order { id: String, total: i64 }

#[tokio::main]
async fn main() -> Result<(), whitewater::SdkError> {
    let client = Client::http(
        vec!["http://localhost:7371".to_owned()],
        "whitewater-local-development-admin-key",
    )?;

    // A Domain is the typed namespace that owns its feeds.
    client.wcl("CREATE DOMAIN shop").await?;
    let shop = client.get_domain("shop").await?;
    shop.create_feed("orders").await?;    // CREATE FEED shop.orders;

    // Typed feed handle — utf8 keys, JSON payloads — pure name
    // composition, no I/O.
    let orders = shop.feed_with("orders", Codec::utf8(), Codec::<Order>::json())?;

    // Everything a service needs hangs off the Domain — relative
    // names resolve inside it.
    let writer = shop
        .create_writer(&orders)?             // &Feed<K,V> keeps its codecs
        .writer_id("checkout")
        .open()
        .await?;
    writer.append(&"order-1".to_owned(), &Order {
        id: "o-1".to_owned(),
        total: 4200,
    }).await?;
    // name form works too: shop.create_writer("orders")? binds utf8 strings

    // ReaderBuilder: durable consume identity (the Kafka groupId
    // analogue) plus a first-time start position.
    let mut reader = shop
        .create_reader(&orders)?
        .reader_id("audit")
        .reader_start_point(ReaderStart::Beginning)
        .max_page_size(64)
        .open()
        .await?;
    // Per-record pump — deliver each message, ack each handled page:
    reader
        .for_each(|m| async move {
            handle(&m).await?;
            Ok(true)                         // false stops, leaving the page unacked
        })
        .await?;
    // or stay manual:
    let page = reader.fetch(16).await?;
    for message in &page.records {
        println!("{:?} -> {:?}", message.key, message.value.id);
    }
    reader.ack().await?;
    reader.close().await?;
    Ok(())
}
```

## Domains

A `Domain` is the strongly-typed administrative namespace that owns
every feed under its name prefix (`shop` owns `shop.orders`):

```rust
let shop = client.domain("shop")?;        // lazy handle — no I/O
let shop = client.get_domain("shop").await?; // eager — verifies + loads
shop.create_feed("orders").await?;      // CREATE FEED shop.orders;
```

The first catalog accessor materializes one batched control read —
`DESCRIBE DOMAIN` plus every `SHOW` listing — and caches it;
`domain.refresh().await?` re-pulls. Scoped views:

| Accessor | Contents |
|---|---|
| `domain_id()` / `describe()` | immutable id, status, history policy |
| `feeds()` / `feeds_prefixed(..)` / `feed_names()` | feeds in this Domain |
| `readers()` / `writers()` | named sessions on this Domain's feeds |
| `subscriptions()` / `pipes()` / `indexes()` / `state_stores()` | scoped resources |

`domain.create_feed("orders")` issues `CREATE FEED shop.orders` —
the scoped companion of `wcl("CREATE FEED shop.orders")`. The Domain is
the only front door: Feeds, Writers, Readers, Subscriptions, Pipes,
StateStores, and Indexes are always declared inside one —
`domain.create_reader("orders")` / `domain.create_writer("orders")`
take relative names or a `Feed` handle and refuse a feed from another
Domain before any wire call.

Reader identity is durable — like Kafka's `groupId`: acknowledged
progress survives restarts under the same `reader_id`.
`reader_start_point(..)` applies only on first declaration; an existing
Reader resumes its acknowledged frontier.

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
shop.declared_feed_schema("orders").await?;
shop.clear_feed_schema("orders").await?;   // reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments. Formats today: `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`,
or `custom.<name>` — compatibility enforcement is still a no-op.

## Codecs

`Codec<T>` is a bidirectional value contract — `encode`/`decode` once,
every operation uses it. Built-ins:

| Codec | Rust type | Wire bytes |
|---|---|---|
| `Codec::utf8()` | `String` | UTF-8 text |
| `Codec::bytes()` | `Vec<u8>` | unchanged |
| `Codec::<T>::json()` | `T: Serialize + DeserializeOwned` | JSON text |
| `Codec::custom(name, enc, dec)` | any | protobuf, Avro, MessagePack, framing, … |

`domain.feed(name)` and `domain.store(name)` bind the cross-SDK
defaults (utf8 keys, raw byte payloads). `feed_with`/`store_with` bind
explicit codecs; `feed.view(kc, vc)`/`store.view(kc, vc)` rebind without
a wire call. Codec failures surface as `SdkError::Codec` — typed and
non-retryable, since an unserializable value is never ambiguous
server-side.

The byte-level `domain.writer`/`reader`/`state_store`/`records` surface
remains the explicit escape hatch and carries the full wire contract —
nanosecond timestamps as `i64`, opaque cursors, record Metadata as raw
bytes.

## Permanent consumer: `feed.consumer()` — supervision inside

For the Kafka-style "consume forever" loop, `feed.consumer(reader)`
wraps the Reader lifecycle in a supervisor — session re-opens,
transient backoff, graceful drain are all internal. Application code
only sees decoded records, one at a time:

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

Value-level store lookups skip the row/freshness wrapper when it does
not matter:

```rust
let profile = profiles.find(&email).await?
    .or(profiles.find_by("user_id", TypedValue::Text(uid)).await?);
let n = counters.get_or_default(&bucket, 0).await?;   // counters, one line
```

## API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`commands`, `wcl`, and the byte-level `Writer`/`Reader`/`StateStore`/
`Member` types are the explicit escape hatches for the same wire
contract.

| Area | API |
|---|---|
| Client & transport | `Client::http(endpoints, api_key)` `Client::new` · `use_compression` `with_api_key` `wire` · `health` `check_compatible` `transport_limits` |
| Typed Domains | `create_domain(name)` declare+handle · `domain(name)` lazy / `get_domain(name)` eager · `name` `domain_id` `describe` `refresh` · `feeds` `feeds_prefixed` `feed_names` `readers` `writers` `subscriptions` `pipes` `indexes` `state_stores` · `feed` `feed_with` `get_feed` `create_feed` `create_reader` `create_writer` · `subscription` `create_subscription` `member` · `writer` `reader` `records` `temporary_fetch*` · `store` `store_with` `state_store` `state_store_raw` `define_state_store` `drop_state_store` · `pipe` `pipe_infos` `define_forward_pipe` `define_enrich_pipe` `define_window_pipe` · `query_index` `set_feed_schema` `clear_feed_schema` — the only front door for resources |
| Schemas | `set_domain_schema` `clear_domain_schema` · `domain.set_feed_schema` `clear_feed_schema` · `Domain::schema` `feed_schema` `declared_feed_schema` · `SchemaDecl`/`FeedSchema` — `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` |
| Feeds | `domain.feed(name)` / `feed_with(name, Codec, Codec)` → `Feed<K,V>` · `use_compression` · `records` `fetch` `load` · `writer` `append` `reader` `consumer` `subscribe` `view` |
| Writing | `FeedWriter::append` `append_event` `append_batch` · `reopen` `session_epoch` · idempotent `request_id` · `FeedEvent` carries event time + Metadata |
| Reading | `FeedReader::for_each` pump · `fetch` `ack` `close` · `session_epoch` `acknowledged_cursor` · durable `reader_id` · `ReaderStart` — `Beginning`, `Now`, `Cursor`, `TimestampNs` |
| Temporary fetches | `domain.temporary_fetch` / `Feed::fetch` — `after_event_time_ns` seeks, `new_only`, `tail` + `wait_ms` long-poll · `Feed::records` `Feed::load` |
| Consumers | `feed.consumer(reader)` → `FeedConsumer`/`ConsumerHandle`: `start_at` `capacity` `backoff` `on_error` `handle` `next` `run` `stop` `wait` `close` |
| Subscription members | `domain.create_subscription(name, feed, start)` — the source Feed may cross Domains via `Scoped::abs`/a `Feed` handle · `domain.member(subscription)` → `Member`: `member_id` `member_epoch` `claim` `fetch` `ack` `renew` `release` — shared progress + RF3 work leases · `Feed::subscribe` managed loop |
| StateStores | typed `store`/`store_with` → `Store<K,V>`: `put` `put_named` `put_keys` `put_with_key` `get` `get_by` `find` `find_by` `get_or_default` `delete` `clear` `iterate` `sync` `rebuild` `view` `raw` · byte-level `state_store`/`state_store_raw` → `StateStore`: `put_row` `delete_row` `clear` `lookup` `get_by` `iterate` `sync` `rebuild` |
| Indexes | `domain.query_index(name, IndexQuery)` → `IndexPage` · `IndexBound` `IndexQuery` `TypedValue` |
| Pipes | `domain.define_forward_pipe` `define_enrich_pipe` `define_window_pipe` — every endpoint a `Scoped` (`Scoped::rel`/`Scoped::abs`/`Feed`/`Store`/`StateStore`/`SubscriptionRef` handles); Pipe + input Subscription stay in-domain · `domain.pipe(name)` → `describe` `pause` `resume` `drop` · `domain.pipe_infos()` (`PipeInfo`) |
| StateStore declare | `domain.define_state_store(StateStoreSpec, source)` — `add_unique_key`/`add_non_unique_key` (`*_with` + `Extractor` on feed-sourced) · `MAX_NAMED_KEYS` |
| Observability | `demand_feeds` `demand_readers` `demand_subscriptions` `demand_indexes` `demand_pipes` → `DemandReport`/`DemandRow::counter` · `hot_keys` → `HotKey` (`feed` `share_bps` `window_records` `window_bytes`) |
| Escape hatches | `wcl(script)` · `commands(Vec<Value>)` raw command batch · `Writer`/`Reader`/`Member`/`StateStore` byte-level sessions |
| Errors | `SdkError` — `Refused` `StaleEpoch` `Sequence` `LeaseBusy` `LeaseStale` `LeaseCapacity` `Conflict` `Ambiguous` `Unavailable` `IndexBehind` `IncompatibleServer` |
| Testing | `Transport` trait seam — inject a scripted stub for the full client stack; see `tests/domain_tests.rs` for the pattern |

## Observability

`client.demand_feeds()` et al. return `DemandReport` — per-resource
`DemandRow`s (name, domain, verbatim counter fields) plus the per-domain
rollup. Counter names are a server-owned vocabulary; `row.counter("x")`
reads any field. `client.hot_keys()` reports dominant-Key evidence — the
operator signal to spread or shed load, since a strictly ordered hot Key
is never parallelized.

## Error contract

`SdkError` maps wire `(status, code)` to typed variants:
`StaleEpoch` (reopen the session), `LeaseBusy`/`LeaseStale`/
`LeaseCapacity` (subscription work fencing), `IndexBehind` (strict
freshness unreachable in the bounded wait), `Unavailable`/`Ambiguous`/
`Transport` (`retryable()` — retry the identical request), `Conflict`,
`Refused`, `IncompatibleServer`, `Protocol`, `Codec`.

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
