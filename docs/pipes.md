# Pipes

A **Pipe** is a managed stream job: it drives one declared **Subscription**
and applies an operation to every record the Subscription delivers, writing
results to a Feed or a StateStore. Pipes run server-side — your process
does not need to stay alive for a Pipe to keep going.

| Operation | Reads from | Writes to | Purpose |
|---|---|---|---|
| `forward` | Subscription | Feed | copy records verbatim |
| `enrich` | Subscription + StateStore | Feed (+ optional quarantine Feed) | point-lookup enrichment |
| `window` | Subscription | StateStore (+ optional emit Feed) | fixed-window aggregation |

```rust
let client = Client::new(
    Arc::new(HttpTransport::new()?),
    vec!["http://localhost:7371".to_owned()],
    "api-key",
)?;
```

## 1. Prerequisites: subscriptions and stores

Every Pipe names a **Subscription** — the durable delivery lease it pulls
input through. Declare one per Pipe:

```rust
client
    .create_subscription("orders.sub", "ops.orders", None)
    .await?;
// start defaults to ReaderStart::Beginning; ReaderStart::Now /
// Cursor / TimestampNs select other starts.
```

`enrich` and `window` pipes need a **StateStore** (see `docs/stores.md`):

```rust
use whitewater::{Extractor, StateStoreSpec};

let spec = StateStoreSpec::new("ops.customers")?
    .add_key("email", true, Some(Extractor::json("$.email")?))?
    .add_key("tier", false, Some(Extractor::json("$.tier")?))?;
client.define_state_store(spec, Some("ops.customer.changes")).await?;
```

## 2. Forward pipes

```rust
client
    .define_forward_pipe(
        "ops.orders.relay",   // pipe name (dotted)
        "orders.sub",         // source subscription
        "ops.orders.relayed", // output feed
    )
    .await?;
```

## 3. Enrich pipes

Point-lookup enrichment: project a key with an `Extractor`, resolve it
against the store, and copy the matched row into output Metadata
(`enrich.payload`, `enrich.matched`). Resolution rides inside the effect
journal — replayed outputs are byte-identical.

```rust
use whitewater::{EnrichFreshness, EnrichMiss, Extractor, PipeOperation};

client
    .define_enrich_pipe(
        "ops.orders.enrich",
        "orders.sub",
        "ops.enriched",
        "ops.customers",                 // lookup store
        PipeOperation::enrich(
            Extractor::json("$.customerId")?,
            EnrichFreshness::Latest,     // or EnrichFreshness::Strict
            Some("email"),               // named key; None = primary
            EnrichMiss::Quarantine,      // Forward | Drop | Quarantine
        ),
        Some("ops.enrich.misses"),       // required for Quarantine
    )
    .await?;
```

## 4. Window pipes

A window pipe's output store must be declared before the pipe — and it
must carry non-unique named keys `"key"` and `"pane"` so pane rows stay
enumerable without composite row keys:

```rust
use whitewater::{PipeOperation, WindowAggregation, WindowTime};

let panes = StateStoreSpec::new("ops.order.counts")?
    .add_key("key", false, None)?
    .add_key("pane", false, None)?;
client.define_state_store(panes, None).await?;

client
    .define_window_pipe(
        "ops.orders.hourly",
        "orders.sub",
        "ops.order.counts",              // pane store (declared above)
        PipeOperation::window(
            Duration::from_secs(3600),     // 1h panes
            Extractor::json("$.region")?,
            WindowAggregation::sum(Extractor::json("$.total")?),
        )?
        .time(WindowTime::Event)           // or WindowTime::Ingest
        .watermark(Duration::from_secs(300))   // event-time close horizon (5m)
        .grace(Duration::from_secs(60))        // revise emitted panes up to 1m late
        .expire(Duration::from_secs(86_400))   // keep panes 24h
        .key_limit(10_000),
        Some("ops.hourly.emits"),        // optional closed-pane output
    )
    .await?;
```

Window durations are `std::time::Duration`s — the SDK extracts
nanoseconds internally for the wire; callers never spell raw nanosecond
counts.

### Lateness models — pick exactly one

- `max_late` — ingest-clock bound; later arrivals quarantine into a
  countable `late` row.
- `watermark` — event-time horizon `max(event_time) − watermark`
  closes a pane once it passes the pane end (refused under
  `WindowTime::Ingest`); `grace` lets late events revise an emitted
  pane before counting into `late`.

An emit feed requires a declared lateness model — without
`max_late`/`watermark` no pane ever provably closes and the define
refuses locally. `expire` must exceed `grace` and `max_late`
when they are declared; `key_limit`/`byte_limit` cap pane state at
`WINDOW_MAX_KEY_LIMIT` (512) keys and `WINDOW_MAX_BYTE_LIMIT` (256 KiB).

### Aggregations

```rust
WindowAggregation::count();
WindowAggregation::sum(Extractor::json("$.total")?);
WindowAggregation::average(Extractor::json("$.rtt")?);
```

## 5. Lifecycle

```rust
let pipe = client.pipe("ops.orders.relay");

let info = pipe.describe().await?;   // stage, operation, outputs, status
pipe.pause().await?;                 // cooperative; the frontier stays put
pipe.resume().await?;                // restarts from the durable frontier
pipe.drop().await?;

let all_pipes = client.pipes().await?;
```

`pause` is durable and idempotent — nothing is dropped while paused.
`describe()` returns `stage` (`declared`/`paused`), the opaque `operation`
node, and output Feed/Store IDs.

## 6. Watching demand

```rust
let demand = client.demand_pipes().await?;
for row in &demand.rows {
    println!("{} drives={:?}", row.name, row.counter("drives"));
}
```

## 7. Failure semantics

- Declarations commit atomically inside `client.commands(vec![...])` —
  submit the subscription and `define_pipe` in one batch to apply both or
  neither.
- Pipe delivery is at-least-once; effects journal lookups/accumulation so
  replayed outputs are byte-identical.
