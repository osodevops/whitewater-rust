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
- **Indexes, Pipes, StateStores** — typed index queries, Pipe
  describe/pause/resume/drop, manual and feed-sourced stores with
  freshness evidence.
- **Demand observability** — per-resource counters and per-domain
  rollups (`demand_feeds`, `demand_readers`, `demand_subscriptions`,
  `demand_indexes`, `demand_pipes`, `hot_keys`).

## Quick start

```rust
use whitewater::{Client, Codec, FeedEvent, TemporaryFetch};

#[derive(serde::Serialize, serde::Deserialize)]
struct Order { id: String, total: i64 }

#[tokio::main]
async fn main() -> Result<(), whitewater::SdkError> {
    let client = Client::http(
        vec!["http://localhost:7371".to_owned()],
        "whitewater-local-development-admin-key",
    )?;

    // Bind codecs once: utf8 keys, JSON payloads.
    let orders = client.feed_with(
        "orders.events",
        Codec::utf8(),
        Codec::<Order>::json(),
    );

    // Declares the Writer when missing and opens a fenced session.
    let writer = orders.writer("checkout").await?;
    writer.append(&"order-1".to_owned(), &Order {
        id: "o-1".to_owned(),
        total: 4200,
    }).await?;

    // Reader: decoded pages; ack commits durable progress.
    let mut reader = orders.reader("audit", 64).await?;
    let page = reader.fetch(16).await?;
    for message in &page.records {
        println!("{:?} -> {:?}", message.key, message.value.id);
    }
    reader.ack().await?;
    reader.close().await?;
    Ok(())
}
```

## Codecs

`Codec<T>` is a bidirectional value contract — `encode`/`decode` once,
every operation uses it. Built-ins:

| Codec | Rust type | Wire bytes |
|---|---|---|
| `Codec::utf8()` | `String` | UTF-8 text |
| `Codec::bytes()` | `Vec<u8>` | unchanged |
| `Codec::<T>::json()` | `T: Serialize + DeserializeOwned` | JSON text |
| `Codec::custom(name, enc, dec)` | any | protobuf, Avro, MessagePack, framing, … |

`client.feed(name)` and `client.store(name)` bind the cross-SDK
defaults (utf8 keys, raw byte payloads). `feed_with`/`store_with` bind
explicit codecs; `feed.view(kc, vc)`/`store.view(kc, vc)` rebind without
a wire call. Codec failures surface as `SdkError::Codec` — typed and
non-retryable, since an unserializable value is never ambiguous
server-side.

The byte-level `client.writer`/`reader`/`state_store`/`records` surface
remains the explicit escape hatch and carries the full wire contract —
nanosecond timestamps as `i64`, opaque cursors, record Metadata as raw
bytes.

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
  store.rs        byte-level StateStore + freshness/outcome types
  pipe.rs         declared Pipe lifecycle
  typed.rs        TypedValue + IndexQuery/IndexPage
  demand.rs       demand counters + hot-Key evidence
  codec.rs        Codec<T> — utf8 / bytes / json / custom
  feed.rs         Feed<K,V>, FeedWriter, FeedReader, subscribe
  typed_store.rs  Store<K,V>
  error.rs        SdkError + wire status/code mapping
  transport.rs    Transport trait + HttpTransport (reqwest/rustls)
  model.rs        Record/AppendOutcome/pages + wire decoders
examples/
  typed_feed.rs   end-to-end typed write/read against a dev Node
```
