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
            body: json!({ "results": [{ "status": "ok", "data": {} }] }),
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
        .define_enrich_pipe(
            "accounts.enrich",
            "accounts.flow",
            "accounts.enriched",
            "accounts.users",
            operation,
            Some("accounts.misses"),
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
        .define_window_pipe(
            "accounts.rollup",
            "accounts.flow",
            "accounts.panes",
            operation,
            Some("accounts.closed"),
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
        .define_enrich_pipe("p", "s", "o", "st", PipeOperation::forward(), None)
        .await
        .is_err());
    assert!(client
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
        .define_enrich_pipe("p", "s", "o", "st", quarantine_miss, None)
        .await
        .is_err());
    let forward_miss = PipeOperation::enrich_default(Extractor::record_key());
    assert!(client
        .define_enrich_pipe("p", "s", "o", "st", forward_miss, Some("q.feed"))
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
        .define_window_pipe("p", "s", "st", unbounded, Some("out"))
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
        .add_key("passport", true, Some(Extractor::json("passport").unwrap()))
        .unwrap()
        .add_key("region", false, Some(Extractor::record_key()))
        .unwrap();
    client
        .define_state_store(spec, Some("accounts.signups"))
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
        .add_key("department", false, None)
        .unwrap();
    client
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
        .add_key("k", false, Some(Extractor::record_key()))
        .unwrap();
    assert!(client.define_state_store(spec, None).await.is_err());

    // Feed-sourced store missing an extractor refuses.
    let spec = StateStoreSpec::new("s")
        .unwrap()
        .add_key("k", false, None)
        .unwrap();
    assert!(client.define_state_store(spec, Some("f")).await.is_err());

    // Spec-builder bounds refuse locally too.
    assert!(StateStoreSpec::new("  ").is_err());
    assert!(StateStoreSpec::new("s")
        .unwrap()
        .add_key(" ", false, None)
        .is_err());
    assert!(StateStoreSpec::new("s")
        .unwrap()
        .add_key("dup", false, None)
        .unwrap()
        .add_key("dup", false, None)
        .is_err());
    let spec = (0..MAX_NAMED_KEYS).fold(StateStoreSpec::new("s").unwrap(), |spec, i| {
        spec.add_key(format!("k{i}"), false, None).unwrap()
    });
    assert!(spec.add_key("overflow", false, None).is_err());

    assert!(transport.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_subscription_lowers_to_the_wcl_statement() {
    let transport = Arc::new(CaptureTransport {
        seen: Mutex::new(Vec::new()),
    });
    let client = client(transport.clone());

    client
        .create_subscription("orders.sub", "ops.orders", None)
        .await
        .expect("default start");
    assert_eq!(
        transport.seen.lock().unwrap()[0]["script"],
        json!("CREATE SUBSCRIPTION orders.sub FROM ops.orders START AT BEGINNING;")
    );

    client
        .create_subscription(
            "orders.sub2",
            "ops.orders",
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
