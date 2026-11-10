# How the Rust SDK works

Everything a caller can do enters through `Client` — and every HTTP
call funnels through one `Transport` trait object. There are no other
network paths.

## Layers

```
Client ─┬─ wcl / commands / health                     → POST /v1/admin/*
        ├─ domain / set_domain_schema / …              → typed WCL builders
        ├─ feed / feed_with → Feed<K, V>               → /v1/feeds/*, /v1/readers/temporary/fetch
        ├─ writer(name)                                → /v1/writers/append[-batch]
        ├─ reader(name, capacity)                      → /v1/readers/open|fetch|ack|close
        └─ member(subscription)                        → /v1/subscriptions/members/*
     │                      │
  Transport            model.rs / codec.rs
(endpoints,          (request building,
 auth, errors)        frame decode, response mapping)
```

### `Transport`

Holds nothing but one exchange against one Node base URL: `request`
returns `(status, json)`; `request_binary` / `request_frame_page` /
`request_frame_page_get` / `request_meta_frame_page` are the binary
fetch surfaces (`Accept: application/vnd.whitewater.frames` or
`…member-frames`); `request_binary_body` is the FSA1 append surface.
`HttpTransport` is the production reqwest+rustls implementation; the
trait's default methods refuse binary calls so JSON-only scripted
transports keep working — the client falls back to the JSON wire on
that refusal, never on a server error.

`Client` owns the endpoint pool: stateless calls round-robin and hop
on `SdkError::Transport` only. Session-bound calls (`Writer`,
`Reader`, `Member`) **pin** to the Node that opened the session —
Node-local pacing/lease state makes rotation mid-session wrong — and
re-pin only when the pinned Node fails transport-wise.

### Error mapping

Every non-2xx maps through `map_error` on the wire `code` field:
`StaleEpoch`, `Sequence`, `LeaseBusy`/`LeaseStale`/`LeaseCapacity`,
`Conflict`, `Refused`, `Unavailable`, `Ambiguous` (5xx without a
code — the write may or may not have applied; **retry with the same
`request_id`**, never a new one).

### `model.rs` / `frame.rs`

The only places wire bodies are built or decoded. Keys, payloads, and
metadata values are base64 `Vec<u8>` on the JSON wire; `event_time_ns`/
`ingest_time_ns` are `i64` nanoseconds carried as decimal strings.
`Cursor` values are opaque strings — never parsed or ordered. Binary
fetch pages (`FSR2`/`FSR3` stored frames inside the `u32 count +
next_cursor + (cursor, frame)*` envelope) decode through
`frame::FramePageParser`, which parses incrementally as chunks land
and exposes `next_cursor` as soon as the page header completes.

## Resource management — two doors, one room

`client.wcl(script)` sends raw WCL to `POST /v1/admin/wcl`. The typed
methods are thin builders over it — `client.create_resource_tolerating_exists`
serializes `CREATE …` and tolerates an already-declared resource.

Raw script — several statements commit as one request (one
`request_id` dedupes the whole batch):

```rust
client.wcl("
    CREATE DOMAIN shop;
    CREATE FEED shop.orders;
    CREATE WRITER orders.writer TO shop.orders;
    CREATE READER audit FROM shop.orders START AT BEGINNING;
").await?;
```

`client.commands(vec![...])` is the second door — the typed JSON
command path for what WCL does not spell. Typed helpers lower onto it:
`define_state_store(StateStoreSpec, source)` declares a store with
named keys, `define_{forward,enrich,window}_pipe(...)` declare Pipes
with a `PipeOperation`, and `create_subscription(name, feed, start)`
spells the WCL statement directly.

### Typed Domains — the friendly door

A Domain owns every Feed it prefixes (`shop` owns `shop.orders`), so
`client.domain(name)` returns a strongly-typed facade that navigates
the namespace instead of concatenating dotted names:

- `client.domain(name)` — a lazy handle. `domain.feed("orders")` is
  pure name composition (`shop.orders`); no I/O until a catalog
  accessor runs.
- `client.get_domain(name)` — `domain()` plus an immediate snapshot
  load: the Domain's existence is verified up front.
- The first catalog accessor issues ONE batched `commands` call
  (`describe` + `show` for feeds/readers/writers/subscriptions/pipes/
  indexes/state_stores) and caches the snapshot; `domain.refresh()`
  re-pulls.
- Scoped views: `feed_names()`, `feeds[_prefixed]()`, `readers()`,
  `writers()`, `subscriptions()`, `pipes()`, `indexes()`,
  `state_stores()`, `describe()`, `domain_id()`, `schema()`,
  `feed_schema(rel)`.

`domain.create_reader("orders")` / `domain.create_writer("orders")`
return fluent builders instead of executing WCL directly:

```rust
let reader = shop.create_reader("orders")?        // Codec<String,String>
    .reader_id("fulfillment")                      // durable id
    .reader_start_point(ReaderStart::Beginning)     // or Now/Cursor/TimestampNs
    .max_page_size(256)
    .open().await?;                                 // FeedReader<K, V>

let writer = shop.create_writer("orders")?
    .writer_id("orders.ingest")
    .open().await?;                                 // FeedWriter<K, V>
```

Builder semantics:

- `reader_id`/`writer_id` are required — the durable resource identity.
- `reader_start_point` applies only on first declaration; an existing
  Reader resumes its acknowledged frontier.
- `.declare()` provisions without opening a session; `.open()`
  declares-when-missing then opens.
- `FeedReader::for_each(handler)` delivers decoded messages and acks
  each fetched page after its records are handled; an early `false`
  leaves the in-flight page unacked so its tail replays.
- `Feed::consumer(name)` returns a `FeedConsumer` handle built on the
  same open/fetch/ack pump.

## Reading

Three shapes, in increasing durability:

1. **`client.records(feed, after, limit)`** — one-shot committed
   history page (binary-first: `Accept: application/vnd.whitewater.frames`,
   JSON fallback for Nodes that ignore the Accept).
2. **`client.temporary_fetch(fetch)` /
   `temporary_fetch_frames[_pipelined]`** — anonymous ephemeral reader;
   `tail` + `wait_ms` is a server-side long-poll. Continue with
   `next_cursor`, never the last record's cursor.
3. **`client.reader(name, capacity)`** — durable independent cursor:
   `open` → `fetch`/`fetch_and_ack` → `ack` → `close`. Progress
   survives restarts; `fetch` requests the binary page and updates
   `delivered_cursor`/`acknowledged_cursor`/pacing advice from the
   `x-whitewater-*` response headers.

`Reader::autotuned()` wraps the session in an `AutoReader` that grows
the page limit while pages return full — the read-side sibling of the
writer's `Batching`/`AutoTune` tuning.

`client.member(subscription)` is the lease vocabulary for cooperating
consumers: `join` → `claim`/`renew`/`release` → `fetch` (the
`member-frames` wire: `u32 meta_len` + metadata JSON + frame page,
with `cursor`/`positions`/`delivery_attempt`/`next_delivery_ns`
carried back) → `ack`.

## Writing

- **Named session**: `client.writer(name)` opens `OPEN WRITER SESSION`
  and pins the returned `session_epoch`; `writer.append` /
  `append_batch` dedup on `request_id` and can speak the FSA1 binary
  append body.
- **Batching**: `writer.batched(Batching { linger, max_batch,
  max_in_flight })` returns a `PumpedWriter` — per-Key order is
  preserved while independent Keys pipeline up to `max_in_flight`.
  `AutoTune` drives the same pump adaptively.
- **Ad-hoc**: `feed.append` / `feed.load` — ephemeral writer identity;
  same request-id dedup rule.

## Testing

Unit tests stub the wire with scripted `Transport` implementations —
every call lands in a FIFO reply queue and is recorded for wire-shape
assertions; no live Riverbed required (`tests/sdk_tests.rs`,
`tests/consumer_tests.rs`, `tests/binary_fetch_tests.rs`).
`tests/conformance` replays the shared `sdk/spec/conformance` fixtures
when `WHITEWATER_ENDPOINTS` + `WHITEWATER_SPEC_DIR` are set; skipped
otherwise.
