//! Domain façade tests over the scripted-transport seam — every call
//! lands in a FIFO reply queue and is recorded for wire-shape
//! assertions, matching `tests/sdk_tests.rs`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use whitewater::*;

struct StubTransport {
    replies: Mutex<std::collections::VecDeque<(u16, Value)>>,
    requests: Mutex<Vec<(String, String, Value)>>,
}

impl StubTransport {
    fn new(replies: Vec<(u16, Value)>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn take_requests(&self) -> Vec<(String, String, Value)> {
        std::mem::take(&mut self.requests.lock().unwrap())
    }
}

#[async_trait]
impl Transport for StubTransport {
    async fn request(
        &self,
        _base: &str,
        method: &str,
        path: &str,
        body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        self.requests.lock().unwrap().push((
            method.to_owned(),
            path.to_owned(),
            body.cloned().unwrap_or(Value::Null),
        ));
        let (status, body) = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or((500, json!({"error": "no scripted reply"})));
        Ok(TransportReply { status, body })
    }
}

fn client(replies: Vec<(u16, Value)>) -> (Client, Arc<StubTransport>) {
    let transport = Arc::new(StubTransport::new(replies));
    let client = Client::new(
        transport.clone(),
        vec!["http://node1".to_owned()],
        "test-key",
    )
    .expect("test client");
    (client, transport)
}

const DOMAIN_ID: &str = "11111111-1111-1111-1111-111111111111";
const FEED_ID: &str = "22222222-2222-2222-2222-222222222222";

/// The batched `Domain::load` reply — describe + the seven SHOW
/// listings, with one row in and one row out of scope each.
fn catalog_reply() -> (u16, Value) {
    (
        200,
        json!({
            "revision": 7,
            "results": [
                { "data": {
                    "space_id": DOMAIN_ID,
                    "name": "shop",
                    "status": "active",
                    "created_at_ns": "1700000000000000000"
                } },
                { "data": [
                    { "name": "shop.orders", "feed_id": FEED_ID, "space_id": DOMAIN_ID },
                    { "name": "shop.order.archive", "feed_id": "33333333-3333-3333-3333-333333333333", "space_id": DOMAIN_ID },
                    { "name": "other.feed", "feed_id": "44444444-4444-4444-4444-444444444444", "space_id": "55555555-5555-5555-5555-555555555555" }
                ] },
                { "data": [
                    { "name": "fulfillment", "feed_id": FEED_ID },
                    { "name": "outside-reader", "feed_id": "44444444-4444-4444-4444-444444444444" }
                ] },
                { "data": [
                    { "name": "ingest", "feed_id": FEED_ID },
                    { "name": "outside-writer", "feed_id": "44444444-4444-4444-4444-444444444444" }
                ] },
                { "data": [
                    { "name": "shop.pipeline.sub", "space_id": DOMAIN_ID },
                    { "name": "other.sub", "space_id": "55555555-5555-5555-5555-555555555555" }
                ] },
                { "data": [
                    { "name": "shop.pipeline", "space_id": DOMAIN_ID },
                    { "name": "other.pipe", "space_id": "55555555-5555-5555-5555-555555555555" }
                ] },
                { "data": [
                    { "name": "shop.by_order", "space_id": DOMAIN_ID },
                    { "name": "other.index", "space_id": "55555555-5555-5555-5555-555555555555" }
                ] },
                { "data": [
                    { "name": "shop.inventory", "space_id": DOMAIN_ID },
                    { "name": "other.store", "space_id": "55555555-5555-5555-5555-555555555555" }
                ] }
            ]
        }),
    )
}

fn open_reader_reply() -> (u16, Value) {
    (
        200,
        json!({ "session_epoch": 3, "delivered_cursor": null, "acknowledged_cursor": null }),
    )
}

fn open_writer_reply() -> (u16, Value) {
    (
        200,
        json!({ "results": [ { "data": { "session_epoch": 9 } } ] }),
    )
}

#[tokio::test]
async fn domain_is_lazy_until_a_catalog_accessor_runs() {
    let (client, transport) = client(vec![]);
    let shop = client.domain("shop").unwrap();
    let feed = shop.feed("orders").unwrap();
    assert_eq!(feed.name(), "shop.orders");
    assert!(transport.take_requests().is_empty());
}

#[tokio::test]
async fn get_domain_materializes_the_snapshot_in_one_batched_read() {
    let (client, transport) = client(vec![catalog_reply()]);
    let shop = client.get_domain("shop").await.unwrap();

    assert_eq!(shop.domain_id().await.unwrap(), DOMAIN_ID);
    assert_eq!(
        shop.feed_names().await.unwrap(),
        vec!["shop.order.archive", "shop.orders"]
    );
    assert_eq!(shop.readers().await.unwrap(), vec!["fulfillment"]);
    assert_eq!(shop.writers().await.unwrap(), vec!["ingest"]);
    assert_eq!(
        shop.subscriptions().await.unwrap(),
        vec!["shop.pipeline.sub"]
    );
    assert_eq!(shop.pipes().await.unwrap(), vec!["shop.pipeline"]);
    assert_eq!(shop.indexes().await.unwrap(), vec!["shop.by_order"]);
    assert_eq!(shop.state_stores().await.unwrap(), vec!["shop.inventory"]);

    // Every accessor served from the cache — exactly one control read.
    let requests = transport.take_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].1, "/v1/admin/commands");
    let commands = requests[0].2["commands"].as_array().unwrap();
    assert_eq!(commands[0]["command"], "describe");
    assert_eq!(commands[0]["kind"], "domain");
    assert_eq!(commands[0]["name"], "shop");
}

#[tokio::test]
async fn feeds_returns_typed_handles_and_honours_the_prefix_filter() {
    let (client, _) = client(vec![catalog_reply()]);
    let shop = client.get_domain("shop").await.unwrap();

    let all = shop.feeds().await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].name(), "shop.order.archive");
    assert_eq!(all[1].name(), "shop.orders");

    let order = shop.feeds_prefixed(Some("orders")).await.unwrap();
    assert_eq!(order.len(), 1);
    assert_eq!(order[0].name(), "shop.orders");
}

#[tokio::test]
async fn feed_name_composition_accepts_qualified_and_relative_forms() {
    let (client, _) = client(vec![]);
    let shop = client.domain("shop").unwrap();
    assert_eq!(shop.feed("orders").unwrap().name(), "shop.orders");
    assert_eq!(shop.feed("shop.orders").unwrap().name(), "shop.orders");
    assert!(matches!(shop.feed("Orders-Cap"), Err(SdkError::Refused(_))));
    assert!(client.domain("Bad Name").is_err());
}

#[tokio::test]
async fn get_feed_verifies_declaration_against_the_snapshot() {
    let (client, _) = client(vec![catalog_reply()]);
    let shop = client.get_domain("shop").await.unwrap();
    assert_eq!(shop.get_feed("orders").await.unwrap().name(), "shop.orders");
    assert!(matches!(
        shop.get_feed("missing").await,
        Err(SdkError::Refused(_))
    ));
}

#[tokio::test]
async fn a_cross_domain_feed_is_refused_before_any_network_call() {
    let (client, transport) = client(vec![]);
    let shop = client.domain("shop").unwrap();
    let outside = client.feed("other.orders");
    assert!(matches!(
        client.create_reader(&shop, &outside),
        Err(SdkError::Refused(_))
    ));
    assert!(matches!(
        client.create_writer(&shop, &outside),
        Err(SdkError::Refused(_))
    ));
    assert!(transport.take_requests().is_empty());
}

#[tokio::test]
async fn reader_builder_requires_the_durable_id() {
    let (client, transport) = client(vec![]);
    let shop = client.domain("shop").unwrap();
    let orders = shop.feed("orders").unwrap();
    assert!(matches!(
        client.create_reader(&shop, &orders).unwrap().open().await,
        Err(SdkError::Refused(_))
    ));
    assert!(transport.take_requests().is_empty());
}

#[tokio::test]
async fn reader_builder_declares_with_start_then_opens_a_session() {
    let (client, transport) = client(vec![
        (200, json!({ "results": [] })), // CREATE READER ... tolerated
        open_reader_reply(),
    ]);
    let shop = client.domain("shop").unwrap();
    let reader = client
        .create_reader(&shop, &shop.feed("orders").unwrap())
        .unwrap()
        .reader_id("fulfillment")
        .reader_start_point(ReaderStart::Cursor("c1".to_owned()))
        .max_page_size(64)
        .open()
        .await
        .unwrap();

    let requests = transport.take_requests();
    let script = requests[0].2["script"].as_str().unwrap();
    assert_eq!(
        script,
        "CREATE READER fulfillment FROM shop.orders START AT CURSOR 'c1'"
    );
    assert_eq!(requests[1].1, "/v1/readers/open");
    assert_eq!(requests[1].2["reader"], "fulfillment");
    assert_eq!(requests[1].2["capacity"], 64);
    assert_eq!(reader.session_epoch(), 3);
}

#[tokio::test]
async fn an_existing_reader_resumes_its_acknowledged_frontier() {
    // The second open of the same Reader id tolerates the existing
    // declaration — durable progress wins over the declared start.
    let (client, transport) = client(vec![
        (200, json!({ "results": [] })),
        open_reader_reply(),
        (
            409,
            json!({ "error": "reader 'fulfillment' already exists" }),
        ),
        (
            200,
            json!({
                "session_epoch": 4,
                "delivered_cursor": "d9",
                "acknowledged_cursor": "a7"
            }),
        ),
    ]);
    let shop = client.domain("shop").unwrap();

    let first = client
        .create_reader(&shop, &shop.feed("orders").unwrap())
        .unwrap()
        .reader_id("fulfillment")
        .reader_start_point(ReaderStart::Beginning)
        .open()
        .await
        .unwrap();
    assert_eq!(first.session_epoch(), 3);

    let second = client
        .create_reader(&shop, &shop.feed("orders").unwrap())
        .unwrap()
        .reader_id("fulfillment")
        .reader_start_point(ReaderStart::Beginning)
        .open()
        .await
        .unwrap();
    assert_eq!(second.session_epoch(), 4);

    let requests = transport.take_requests();
    assert_eq!(requests.len(), 4);
}

#[tokio::test]
async fn declare_without_open_sends_the_create_alone() {
    let (client, transport) = client(vec![(200, json!({ "results": [] }))]);
    let shop = client.domain("shop").unwrap();
    client
        .create_reader_named(&shop, "orders")
        .unwrap()
        .reader_id("fulfillment")
        .declare()
        .await
        .unwrap();
    let requests = transport.take_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].2["script"],
        "CREATE READER fulfillment FROM shop.orders START AT BEGINNING;"
    );
}

#[tokio::test]
async fn writer_builder_declares_and_opens_a_session() {
    let (client, transport) = client(vec![
        (200, json!({ "results": [] })), // CREATE WRITER ... tolerated
        open_writer_reply(),
    ]);
    let shop = client.domain("shop").unwrap();
    let writer = client
        .create_writer_named(&shop, "orders")
        .unwrap()
        .writer_id("ingest")
        .open()
        .await
        .unwrap();

    let requests = transport.take_requests();
    assert_eq!(
        requests[0].2["script"],
        "CREATE WRITER ingest TO shop.orders"
    );
    assert_eq!(requests[1].2["script"], "OPEN WRITER SESSION ingest;");
    assert_eq!(writer.session_epoch(), 9);
}

#[tokio::test]
async fn an_undeclared_domain_surfaces_the_server_refusal() {
    let (client, _) = client(vec![(
        400,
        json!({ "error": "domain 'ghost' is not declared" }),
    )]);
    assert!(matches!(
        client.get_domain("ghost").await,
        Err(SdkError::Refused(_))
    ));
}

#[tokio::test]
async fn refresh_repulls_the_catalog_snapshot() {
    let (client, transport) = client(vec![
        catalog_reply(),
        catalog_reply(), // second pull — same shape
    ]);
    let shop = client.get_domain("shop").await.unwrap();
    assert_eq!(shop.feed_names().await.unwrap().len(), 2);
    shop.refresh().await.unwrap();
    assert_eq!(transport.take_requests().len(), 2);
}

/// Catalog reply carrying schema rows: the Domain declares a json value
/// default, `shop.orders` overrides only the key side, `shop.returns`
/// inherits wholesale.
fn schema_catalog_reply() -> (u16, Value) {
    (
        200,
        json!({
            "revision": 9,
            "results": [
                { "data": {
                    "space_id": DOMAIN_ID,
                    "name": "shop",
                    "status": "active",
                    "created_at_ns": "1700000000000000000",
                    "schema": {"value": {"format": "json", "subject": "order-v1"}}
                } },
                { "data": [
                    { "name": "shop.orders", "feed_id": FEED_ID, "space_id": DOMAIN_ID,
                      "schema": {"key": {"format": "text"}},
                      "effective_schema": {"key": {"format": "text"}, "value": {"format": "json", "subject": "order-v1"}} },
                    { "name": "shop.returns", "feed_id": "66666666-6666-6666-6666-666666666666", "space_id": DOMAIN_ID,
                      "schema": null,
                      "effective_schema": {"value": {"format": "json", "subject": "order-v1"}} },
                    { "name": "other.feed", "feed_id": "44444444-4444-4444-4444-444444444444", "space_id": "55555555-5555-5555-5555-555555555555" }
                ] },
                { "data": [] },
                { "data": [] },
                { "data": [] },
                { "data": [] },
                { "data": [] },
                { "data": [] }
            ]
        }),
    )
}

#[tokio::test]
async fn feed_schema_layers_feed_over_domain() {
    let (client, _transport) = client(vec![schema_catalog_reply()]);
    let shop = client.get_domain("shop").await.unwrap();

    let domain = shop.schema().await.unwrap().unwrap();
    assert_eq!(domain.value.as_ref().unwrap().format, "json");
    assert_eq!(
        domain.value.as_ref().unwrap().subject.as_deref(),
        Some("order-v1")
    );

    let effective = shop.feed_schema("shop.orders").await.unwrap().unwrap();
    assert_eq!(effective.key.as_ref().unwrap().format, "text");
    assert_eq!(effective.value.as_ref().unwrap().format, "json");

    let declared = shop
        .declared_feed_schema("shop.orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(declared.key.as_ref().unwrap().format, "text");
    assert!(declared.value.is_none());

    let inherited = shop.feed_schema("shop.returns").await.unwrap().unwrap();
    assert_eq!(inherited.value.as_ref().unwrap().format, "json");
    assert!(inherited.key.is_none());
    assert!(shop
        .declared_feed_schema("shop.returns")
        .await
        .unwrap()
        .is_none());

    assert!(shop.feed_schema("other.feed").await.unwrap().is_none());
}

#[tokio::test]
async fn set_and_clear_schema_emit_typed_commands() {
    let replies = vec![
        (200, json!({ "revision": 1, "results": [{ "data": {} }] })),
        (200, json!({ "revision": 2, "results": [{ "data": {} }] })),
        (200, json!({ "revision": 3, "results": [{ "data": {} }] })),
    ];
    let (client, transport) = client(replies);

    client
        .set_domain_schema(
            "shop",
            &FeedSchema {
                key: None,
                value: Some(SchemaDecl {
                    format: "json".to_owned(),
                    subject: Some("order-v1".to_owned()),
                    definition: None,
                }),
            },
        )
        .await
        .unwrap();
    client
        .set_feed_schema(
            "shop.orders",
            &FeedSchema {
                key: Some(SchemaDecl {
                    format: "text".to_owned(),
                    subject: None,
                    definition: None,
                }),
                value: None,
            },
        )
        .await
        .unwrap();
    client.clear_feed_schema("shop.orders").await.unwrap();

    let requests = transport.take_requests();
    let commands: Vec<Value> = requests
        .iter()
        .filter(|(_, path, _)| path == "/v1/admin/commands")
        .map(|(_, _, body)| body.clone())
        .collect();
    assert_eq!(commands.len(), 3);
    assert_eq!(commands[0]["commands"][0]["command"], "set_schema");
    assert_eq!(commands[0]["commands"][0]["scope"]["kind"], "domain");
    assert_eq!(commands[0]["commands"][0]["scope"]["name"], "shop");
    assert_eq!(
        commands[0]["commands"][0]["schema"]["value"]["format"],
        "json"
    );
    assert_eq!(commands[0]["commands"][0]["schema"]["key"], Value::Null);
    assert_eq!(commands[1]["commands"][0]["scope"]["kind"], "feed");
    assert_eq!(
        commands[1]["commands"][0]["schema"]["key"]["format"],
        "text"
    );
    assert_eq!(commands[2]["commands"][0]["command"], "clear_schema");
    assert_eq!(commands[2]["commands"][0]["scope"]["name"], "shop.orders");
}

/// `Domain::create_feed` composes the Domain prefix and issues the
/// typed `CREATE FEED` WCL — a lazy Domain needs no catalog read first.
#[tokio::test]
async fn create_feed_composes_the_domain_prefix() {
    let (client1, transport) = client(vec![(
        200,
        json!({ "revision": 3, "results": [{ "statement": "create", "message": "ok" }] }),
    )]);
    let shop = client1.domain("shop").expect("lazy domain");

    shop.create_feed("orders").await.expect("create");

    let requests = transport.take_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].1, "/v1/admin/wcl");
    assert_eq!(requests[0].2["script"], "CREATE FEED shop.orders");

    // Malformed names refuse before the wire.
    let (client2, transport2) = client(vec![]);
    let shop2 = client2.domain("shop").expect("lazy domain");
    assert!(shop2.create_feed("BAD.NAME").await.is_err());
    assert!(transport2.take_requests().is_empty());
}

/// Domain-hanging builders resolve relative feeds, and
/// `FeedReader::for_each` acks each handled page — a stopped handler
/// leaves the in-flight page unacked so it replays.
#[tokio::test]
async fn domain_scoped_builders_and_for_each_ack_per_page() {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    let record = |key: &str, payload: &str| {
        json!({
            "message_id": "00000000-0000-0000-0000-000000000000",
            "key_base64": b64.encode(key.as_bytes()),
            "payload_base64": b64.encode(payload.as_bytes()),
            "metadata_base64": {},
            "event_time_ns": "1",
            "ingest_time_ns": "2",
            "cursor": "rc"
        })
    };
    let (client1, transport) = client(vec![
        (200, json!({ "results": [] })), // CREATE WRITER
        (200, json!({ "results": [] })), // CREATE READER
        open_reader_reply(),             // readers/open
        (
            200,
            json!({ "records": [record("a", "1")], "delivered_cursor": "d1",
                    "recommended_capacity": 4, "retry_after_ms": 5 }),
        ),
        (
            200,
            json!({ "session_epoch": 3, "acknowledged_cursor": "d1" }),
        ), // ack p1
        (
            200,
            json!({ "records": [record("b", "2")], "delivered_cursor": "d2" }),
        ),
    ]);
    let shop = client1.domain("shop").unwrap();

    // The writer builder hangs off the Domain too.
    shop.create_writer("orders")
        .unwrap()
        .writer_id("ingest")
        .declare()
        .await
        .unwrap();

    let mut reader = shop
        .create_reader("orders")
        .unwrap()
        .reader_id("fulfillment")
        .reader_start_point(ReaderStart::Beginning)
        .max_page_size(1)
        .open()
        .await
        .unwrap();

    let mut seen = Vec::new();
    reader
        .for_each(|m| {
            seen.push(m.key.clone().unwrap());
            std::future::ready(Ok::<bool, SdkError>(seen.len() < 2))
        })
        .await
        .unwrap();
    assert_eq!(seen, vec!["a".to_owned(), "b".to_owned()]);

    let requests = transport.take_requests();
    assert_eq!(
        requests[0].2["script"],
        "CREATE WRITER ingest TO shop.orders;"
    );
    let acks = requests
        .iter()
        .filter(|(_, path, _)| path == "/v1/readers/ack")
        .count();
    assert_eq!(acks, 1, "page 2 stays unacked so its tail replays");

    // Malformed names refuse before any builder work.
    let (client2, transport2) = client(vec![]);
    let shop2 = client2.domain("shop").unwrap();
    assert!(shop2.create_reader("BAD.NAME").is_err());
    assert!(shop2.create_writer("BAD.NAME").is_err());
    assert!(transport2.take_requests().is_empty());
}
