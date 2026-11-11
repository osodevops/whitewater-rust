//! Declaration wire tests: `define_*_pipe` and `define_state_store`
//! lower the typed operation/spec shapes onto `define_pipe` /
//! `define_state_store` commands, while malformed declarations refuse
//! locally before any round-trip.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use whitewater::*;

/// Captures request bodies so tests assert the exact command wire.
struct CaptureTransport {
    seen: Mutex<Vec<Value>>,
}

#[async_trait]
impl Transport for CaptureTransport {
    async fn request(
        &self,
        _base: &str,
        _method: &str,
        _path: &str,
        body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        self.seen
            .lock()
            .unwrap()
            .push(body.cloned().unwrap_or(Value::Null));
        Ok(TransportReply {
            status: 200,
            body: json!({
                "store_id": "00000000-0000-7000-8000-000000000007",
                "sequence": 4,
                "deduplicated": false,
                "results": [{ "status": "ok", "data": {} }]
            }),
        })
    }
}

fn client(transport: Arc<CaptureTransport>) -> Client {
    Client::new(transport, vec!["http://node1".to_owned()], "api-key").expect("client")
}

#[tokio::test]
async fn define_forward_pipe_lowers_the_typed_operation() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    client(transport.clone())
        .domain("accounts")
        .unwrap()
        .define_forward_pipe("accounts.copy", "accounts.flow", "accounts.out")
        .await
        .expect("forward define");
    let commands = transport.seen.lock().unwrap()[0]["commands"].clone();
    assert_eq!(
        commands[0],
        json!({
            "command": "define_pipe",
            "name": "accounts.copy",
            "subscription": "accounts.flow",
            "output_feed": "accounts.out",
            "operation": { "operation": "forward" },
        })
    );
}

#[tokio::test]
async fn define_enrich_pipe_carries_the_lookup_contract() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    let operation = PipeOperation::enrich(
        Extractor::metadata("user_id").unwrap(),
        EnrichFreshness::Strict,
        Some("passport"),
        EnrichMiss::Quarantine,
    );
    client(transport.clone())
        .domain("accounts")
        .unwrap()
        .define_enrich_pipe(
            "accounts.enrich",
            "accounts.flow",
            "accounts.enriched",
            "accounts.users",
            operation,
            Some(Scoped::rel("accounts.misses")),
        )
        .await
        .expect("enrich define");
    let commands = transport.seen.lock().unwrap()[0]["commands"].clone();
    assert_eq!(
        commands[0],
        json!({
            "command": "define_pipe",
            "name": "accounts.enrich",
            "subscription": "accounts.flow",
            "output_feed": "accounts.enriched",
            "output_store": "accounts.users",
            "quarantine_feed": "accounts.misses",
            "operation": {
                "operation": "enrich",
                "key": { "kind": "metadata", "name": "user_id" },
                "freshness": "strict",
                "named": "passport",
                "on_miss": "quarantine",
            },
        })
    );
}

#[tokio::test]
async fn define_window_pipe_lowers_bounds_and_refuses_emit_without_lateness() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    let client = client(transport.clone());
    let operation = PipeOperation::window(
        Duration::from_secs(30),
        Extractor::json("region").unwrap(),
        WindowAggregation::sum(Extractor::json("total").unwrap()),
    )
    .unwrap()
    .watermark(Duration::from_secs(5))
    .grace(Duration::from_secs(1))
    .expire(Duration::from_secs(600))
    .key_limit(64)
    .byte_limit(1024);
    client
        .domain("accounts")
        .unwrap()
        .define_window_pipe(
            "accounts.rollup",
            "accounts.flow",
            "accounts.panes",
            operation,
            Some(Scoped::rel("accounts.closed")),
        )
        .await
        .expect("window define");
    let commands = transport.seen.lock().unwrap()[0]["commands"].clone();
    assert_eq!(
        commands[0],
        json!({
            "command": "define_pipe",
            "name": "accounts.rollup",
            "subscription": "accounts.flow",
            "output_store": "accounts.panes",
            "emit_feed": "accounts.closed",
            "operation": {
                "operation": "window",
                "window_ns": 30_000_000_000u64,
                "key": { "kind": "projection", "extractor": "json", "version": 1, "field": "region" },
                "aggregation": {
                    "aggregation": "sum",
                    "value": { "kind": "projection", "extractor": "json", "version": 1, "field": "total" },
                },
                "time": "event",
                "watermark_ns": 5_000_000_000u64,
                "grace_ns": 1_000_000_000u64,
                "expire_ns": 600_000_000_000u64,
                "key_limit": 64u64,
                "byte_limit": 1024u64,
            },
        })
    );
}

#[tokio::test]
async fn define_methods_refuse_malformed_declarations_locally() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    let client = client(transport.clone());

    // A forward operation cannot drive an enrich or window define.
    assert!(client
        .domain("accounts")
        .unwrap()
        .define_enrich_pipe("p", "s", "o", "st", PipeOperation::forward(), None)
        .await
        .is_err());
    assert!(client
        .domain("accounts")
        .unwrap()
        .define_window_pipe("p", "s", "st", PipeOperation::forward(), None)
        .await
        .is_err());

    // Quarantine miss requires a quarantine feed — and nothing else takes one.
    let quarantine_miss = PipeOperation::enrich(
        Extractor::record_key(),
        EnrichFreshness::Latest,
        None,
        EnrichMiss::Quarantine,
    );
    assert!(client
        .domain("accounts")
        .unwrap()
        .define_enrich_pipe("p", "s", "o", "st", quarantine_miss, None)
        .await
        .is_err());
    let forward_miss = PipeOperation::enrich_default(Extractor::record_key());
    assert!(client
        .domain("accounts")
        .unwrap()
        .define_enrich_pipe(
            "p",
            "s",
            "o",
            "st",
            forward_miss,
            Some(Scoped::abs("q.feed"))
        )
        .await
        .is_err());

    // emit_feed with no lateness bound can never close a pane.
    let unbounded = PipeOperation::window(
        Duration::from_secs(30),
        Extractor::record_key(),
        WindowAggregation::count(),
    )
    .unwrap();
    assert!(client
        .domain("accounts")
        .unwrap()
        .define_window_pipe("p", "s", "st", unbounded, Some(Scoped::rel("out")))
        .await
        .is_err());

    // grace requires watermark; ingest clock rejects lateness.
    let grace_alone = PipeOperation::window(
        Duration::from_secs(30),
        Extractor::record_key(),
        WindowAggregation::count(),
    )
    .unwrap()
    .grace(Duration::from_secs(1));
    assert!(client
        .domain("accounts")
        .unwrap()
        .define_window_pipe("p", "s", "st", grace_alone, None)
        .await
        .is_err());
    let ingest_late = PipeOperation::window(
        Duration::from_secs(30),
        Extractor::record_key(),
        WindowAggregation::count(),
    )
    .unwrap()
    .time(WindowTime::Ingest)
    .watermark(Duration::from_secs(1));
    assert!(client
        .domain("accounts")
        .unwrap()
        .define_window_pipe("p", "s", "st", ingest_late, None)
        .await
        .is_err());

    // Zero duration and over-ceiling limits refuse before the wire.
    assert!(PipeOperation::window(
        Duration::ZERO,
        Extractor::record_key(),
        WindowAggregation::count()
    )
    .is_err());
    let oversized = PipeOperation::window(
        Duration::from_secs(30),
        Extractor::record_key(),
        WindowAggregation::count(),
    )
    .unwrap()
    .key_limit(WINDOW_MAX_KEY_LIMIT + 1);
    assert!(client
        .domain("accounts")
        .unwrap()
        .define_window_pipe("p", "s", "st", oversized, None)
        .await
        .is_err());

    // Nothing reached the wire.
    assert!(transport.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn define_state_store_lowers_named_keys_and_source() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    let client = client(transport.clone());

    // Feed-sourced store: every named key declares an extractor.
    let spec = StateStoreSpec::new("accounts.users")
        .unwrap()
        .add_unique_key("passport", Some(Extractor::json("passport").unwrap()))
        .unwrap()
        .add_non_unique_key("region", Some(Extractor::record_key()))
        .unwrap();
    client
        .domain("accounts")
        .unwrap()
        .define_state_store(spec, Some("accounts.signups".into()))
        .await
        .expect("feed-sourced define");
    let commands = transport.seen.lock().unwrap()[0]["commands"].clone();
    assert_eq!(
        commands[0],
        json!({
            "command": "define_state_store",
            "name": "accounts.users",
            "source": { "kind": "feed", "feed": "accounts.signups" },
            "named_keys": [
                {
                    "name": "passport",
                    "unique": true,
                    "extractor": { "kind": "projection", "extractor": "json", "version": 1, "field": "passport" },
                },
                {
                    "name": "region",
                    "unique": false,
                    "extractor": { "kind": "key" },
                },
            ],
        })
    );

    // Manual store: named keys carry no extractor.
    let spec = StateStoreSpec::new("accounts.manual")
        .unwrap()
        .add_non_unique_key("department", None)
        .unwrap();
    client
        .domain("accounts")
        .unwrap()
        .define_state_store(spec, None)
        .await
        .expect("manual define");
    let commands = transport.seen.lock().unwrap()[1]["commands"].clone();
    assert_eq!(
        commands[0],
        json!({
            "command": "define_state_store",
            "name": "accounts.manual",
            "source": { "kind": "manual" },
            "named_keys": [{ "name": "department", "unique": false }],
        })
    );
}

#[tokio::test]
async fn define_state_store_refuses_extractor_source_mismatches() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    let client = client(transport.clone());

    // Manual store with an extractor refuses.
    let spec = StateStoreSpec::new("s")
        .unwrap()
        .add_non_unique_key("k", Some(Extractor::record_key()))
        .unwrap();
    assert!(client
        .domain("ops")
        .unwrap()
        .define_state_store(spec, None)
        .await
        .is_err());

    // Feed-sourced store missing an extractor refuses.
    let spec = StateStoreSpec::new("s")
        .unwrap()
        .add_non_unique_key("k", None)
        .unwrap();
    assert!(client
        .domain("ops")
        .unwrap()
        .define_state_store(spec, Some("f".into()))
        .await
        .is_err());

    // Spec-builder bounds refuse locally too.
    assert!(StateStoreSpec::new("  ").is_err());
    assert!(StateStoreSpec::new("s")
        .unwrap()
        .add_non_unique_key(" ", None)
        .is_err());
    assert!(StateStoreSpec::new("s")
        .unwrap()
        .add_non_unique_key("dup", None)
        .unwrap()
        .add_non_unique_key("dup", None)
        .is_err());
    let spec = (0..MAX_NAMED_KEYS).fold(StateStoreSpec::new("s").unwrap(), |spec, i| {
        spec.add_non_unique_key(format!("k{i}"), None).unwrap()
    });
    assert!(spec.add_non_unique_key("overflow", None).is_err());

    // The explicit helpers set the flag the WCL UNIQUE keyword encodes.
    let flags = StateStoreSpec::new("s")
        .unwrap()
        .add_unique_key("passport", None)
        .unwrap()
        .add_non_unique_key("department", None)
        .unwrap();
    assert!(flags.named_keys()[0].unique);
    assert!(!flags.named_keys()[1].unique);
    let feed_sourced = StateStoreSpec::new("s")
        .unwrap()
        .add_unique_key("pp", Some(Extractor::metadata("pp").unwrap()))
        .unwrap()
        .add_non_unique_key("dept", Some(Extractor::json("dept").unwrap()))
        .unwrap();
    assert!(feed_sourced.named_keys()[0].unique);
    assert!(feed_sourced.named_keys()[0].extractor.is_some());
    assert!(!feed_sourced.named_keys()[1].unique);

    assert!(transport.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_subscription_lowers_to_the_wcl_statement() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    let client = client(transport.clone());

    client
        .domain("orders")
        .unwrap()
        .create_subscription("orders.sub", Scoped::abs("ops.orders"), None)
        .await
        .expect("default start");
    assert_eq!(
        transport.seen.lock().unwrap()[0]["script"],
        json!("CREATE SUBSCRIPTION orders.sub FROM ops.orders START AT BEGINNING;")
    );

    client
        .domain("orders")
        .unwrap()
        .create_subscription(
            "orders.sub2",
            Scoped::abs("ops.orders"),
            Some(ReaderStart::Cursor("cur'sor".to_owned())),
        )
        .await
        .expect("cursor start");
    assert_eq!(
        transport.seen.lock().unwrap()[1]["script"],
        json!("CREATE SUBSCRIPTION orders.sub2 FROM ops.orders START AT CURSOR 'cur''sor';")
    );
}

#[tokio::test]
async fn extractor_wire_shapes_match_the_field_spec_contract() {
    assert_eq!(Extractor::record_key().to_wire(), json!({ "kind": "key" }));
    assert_eq!(
        Extractor::metadata("hdr").unwrap().to_wire(),
        json!({ "kind": "metadata", "name": "hdr" })
    );
    assert_eq!(
        Extractor::json("a.b").unwrap().to_wire(),
        json!({ "kind": "projection", "extractor": "json", "version": 1, "field": "a.b" })
    );
    assert!(Extractor::metadata("").is_err());
    assert!(Extractor::json("").is_err());
    assert!(Extractor::metadata("x".repeat(257)).is_err());
    assert!(Extractor::json("x".repeat(513)).is_err());
}

#[derive(Clone, Debug, Default)]
struct PassportKeySet(StoreKeyMap);

impl PassportKeySet {
    fn new() -> Self {
        Self(StoreKeyMap::new())
    }

    fn passport(mut self, v: impl Into<String>) -> Self {
        self.0.set("passport", v.into());
        self
    }

    fn department(mut self, v: impl Into<String>) -> Self {
        self.0.set("department", v.into());
        self
    }

    fn with(passport: impl Into<String>, department: impl Into<String>) -> Self {
        Self::new().passport(passport).department(department)
    }

    fn with_passport(passport: impl Into<String>) -> Self {
        Self::new().passport(passport)
    }

    fn of(builder: impl FnOnce(&mut Self)) -> Self {
        let mut ks = Self::new();
        builder(&mut ks);
        ks
    }
}

impl StoreKeySet for PassportKeySet {
    fn to_named_postings(&self) -> Vec<(String, Vec<TypedValue>)> {
        self.0.to_named_postings()
    }
}

#[tokio::test]
async fn store_key_set_supports_all_inline_styles_and_single_string_fallback() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    let client = client(transport.clone());
    let accounts = client.domain("accounts").unwrap();
    let spec = StateStoreSpec::new("employees")
        .unwrap()
        .add_unique_key("passport", None)
        .unwrap()
        .add_non_unique_key("department", None)
        .unwrap();
    accounts.define_state_store(spec, None).await.unwrap();
    let store = accounts.store("employees").unwrap();
    let id = uuid::Uuid::nil();

    // 1A: Static factory PassportKeySet::with("P123", "eng")
    store
        .put_keys(
            id,
            &"emp-1".to_owned(),
            &"alice".to_owned(),
            &PassportKeySet::with("P123", "eng"),
        )
        .await
        .unwrap();
    // 1B: Static starter + fluent chaining
    store
        .put_keys(
            id,
            &"emp-2".to_owned(),
            &"bob".to_owned(),
            &PassportKeySet::with_passport("P222").department("ops"),
        )
        .await
        .unwrap();
    // 2: Closure / builder block
    store
        .put_keys(
            id,
            &"emp-3".to_owned(),
            &"cara".to_owned(),
            &PassportKeySet::of(|k| {
                *k = k.clone().passport("P333").department("eng");
            }),
        )
        .await
        .unwrap();
    // 3: Single default string fallback -> maps to first declared key ("passport")
    store
        .put_with_key(id, &"emp-4".to_owned(), &"dan".to_owned(), "P444")
        .await
        .unwrap();

    let seen = transport.seen.lock().unwrap();
    assert_eq!(
        seen[1]["op"]["named"],
        json!([
            { "name": "passport", "values": [{ "kind": "text", "value": "P123" }] },
            { "name": "department", "values": [{ "kind": "text", "value": "eng" }] }
        ])
    );
    assert_eq!(
        seen[2]["op"]["named"],
        json!([
            { "name": "passport", "values": [{ "kind": "text", "value": "P222" }] },
            { "name": "department", "values": [{ "kind": "text", "value": "ops" }] }
        ])
    );
    assert_eq!(
        seen[3]["op"]["named"],
        json!([
            { "name": "passport", "values": [{ "kind": "text", "value": "P333" }] },
            { "name": "department", "values": [{ "kind": "text", "value": "eng" }] }
        ])
    );
    assert_eq!(
        seen[4]["op"]["named"],
        json!([{ "name": "passport", "values": [{ "kind": "text", "value": "P444" }] }])
    );
}
