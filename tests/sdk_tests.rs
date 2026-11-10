//! Scripted-transport tests: every call lands in a FIFO reply queue
//! and is recorded for wire-shape assertions — the same seam the
//! server's embedded test suite drives.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

use whitewater::*;

/// Scripted transport: every call lands in a FIFO reply queue and is
/// recorded for wire-shape assertions.
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
        vec!["http://node1".to_owned(), "http://node2".to_owned()],
        "test-key",
    )
    .expect("test client");
    (client, transport)
}

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

fn record_json(key: &[u8], payload: &[u8]) -> Value {
    json!({
        "message_id": Uuid::nil().to_string(),
        "key_base64": B64.encode(key),
        "payload_base64": B64.encode(payload),
        "metadata_base64": {},
        "event_time_ns": "1700000000000000001",
        "ingest_time_ns": "1700000000000000002",
        "cursor": "record-cursor"
    })
}

async fn open_writer(client: &Client, name: &str) -> Writer {
    client.writer(name).await.expect("open writer")
}

async fn open_reader(client: &Client, name: &str) -> Reader {
    client.reader(name, 64).await.expect("open reader")
}

#[test]
fn client_rejects_an_empty_endpoint_pool() {
    let transport = Arc::new(StubTransport::new(vec![]));
    assert!(matches!(
        Client::new(transport, Vec::new(), "k"),
        Err(SdkError::Refused(_))
    ));
}

#[tokio::test]
async fn error_mapping_follows_status_and_code_not_text() {
    for (status, code, want) in [
        (409u16, Some("stale_epoch"), SdkError::StaleEpoch),
        (409, Some("sequence"), SdkError::Sequence),
        (409, Some("lease.busy"), SdkError::LeaseBusy),
        (409, Some("lease.stale_lease"), SdkError::LeaseStale),
        (409, Some("lease.capacity"), SdkError::LeaseCapacity),
        (409, None, SdkError::Conflict("x".to_owned())),
        (413, None, SdkError::PayloadTooLarge("x".to_owned())),
        (503, None, SdkError::Unavailable("x".to_owned())),
        (502, None, SdkError::Ambiguous("x".to_owned())),
        (400, None, SdkError::Refused("400: x".to_owned())),
        (
            401,
            Some("whatever"),
            SdkError::Refused("401: x".to_owned()),
        ),
    ] {
        let body = match code {
            Some(code) => json!({"error": "x", "code": code}),
            None => json!({"error": "x"}),
        };
        let (client, _transport) = client(vec![(status, body)]);
        let error = client.records("orders.events", None, 1).await.unwrap_err();
        assert_eq!(error, want, "{status}/{code:?}");
    }
    assert!(SdkError::Ambiguous(String::new()).retryable());
    assert!(SdkError::Unavailable(String::new()).retryable());
    assert!(!SdkError::StaleEpoch.retryable());
}

#[tokio::test]
async fn transport_limits_decode_advertised_bounds_and_refuse_gaps() {
    let (ready, _transport) = client(vec![(
        200,
        json!({
            "status": "ok",
            "protocol": {"major": 1, "minor": 0},
            "limits": {
                "max_frame_bytes": 67108864,
                "max_committed_read_bytes": 16777216,
                "max_logical_read_frames": 10000,
                "max_logical_read_ranges": 128,
                "subscription_member_fetch_max_limit": 256,
                "subscription_member_max_lease_ticks": 600
            }
        }),
    )]);
    let limits = ready.transport_limits().await.unwrap();
    assert_eq!(limits.max_frame_bytes, 67_108_864);
    assert_eq!(limits.max_committed_read_bytes, 16_777_216);
    assert_eq!(limits.member_fetch_max_limit, 256);
    assert_eq!(limits.member_max_lease_ticks, 600);

    let (missing, _transport) = client(vec![(
        200,
        json!({"status": "ok", "protocol": {"major": 1, "minor": 0}}),
    )]);
    assert!(matches!(
        missing.transport_limits().await,
        Err(SdkError::Protocol(_))
    ));
}

#[tokio::test]
async fn writer_append_carries_epoch_request_id_and_decimal_nanos() {
    let (client, transport) = client(vec![
        (200, json!({"results": [{"data": {"session_epoch": 7}}]})),
        (
            200,
            json!({
                "message_id": Uuid::nil().to_string(),
                "cursor": "cur-1",
                "deduplicated": false,
                "feedback": {"hot_key": true, "retry_after_ms": 250}
            }),
        ),
    ]);
    let writer = open_writer(&client, "orders.wr").await;
    let request_id = Uuid::new_v4();
    let outcome = writer
        .append(
            request_id,
            b"order-1",
            b"created",
            1_700_000_000_000_000_001,
            BTreeMap::from([("source".to_owned(), b"conformance".to_vec())]),
        )
        .await
        .unwrap();
    assert!(!outcome.deduplicated);
    assert!(outcome.hot_key);
    assert_eq!(outcome.retry_after_ms, Some(250));
    let requests = transport.take_requests();
    let (method, path, body) = &requests[1];
    assert_eq!(method, "POST");
    assert_eq!(path, "/v1/writers/append");
    assert_eq!(body["request_id"], request_id.to_string());
    assert_eq!(body["session_epoch"], 7);
    // Nanoseconds ride the wire as a decimal string, never a float.
    assert_eq!(body["event_time_ns"], "1700000000000000001");
    assert_eq!(body["key_base64"], B64.encode(b"order-1"));
    assert_eq!(body["use_compression"], false);
}

#[tokio::test]
async fn append_maps_stale_epoch_for_caller_reopen() {
    let (client, _transport) = client(vec![
        (200, json!({"results": [{"data": {"session_epoch": 3}}]})),
        (409, json!({"error": "superseded", "code": "stale_epoch"})),
    ]);
    let writer = open_writer(&client, "orders.wr").await;
    let error = writer
        .append(Uuid::new_v4(), b"k", b"v", 1, BTreeMap::new())
        .await
        .unwrap_err();
    assert_eq!(error, SdkError::StaleEpoch);
}

#[tokio::test]
async fn writer_reopen_pulls_a_fresh_epoch() {
    let (client, _transport) = client(vec![
        (200, json!({"results": [{"data": {"session_epoch": 3}}]})),
        (200, json!({"results": [{"data": {"session_epoch": 9}}]})),
    ]);
    let mut writer = open_writer(&client, "orders.wr").await;
    writer.reopen().await.unwrap();
    assert_eq!(writer.session_epoch(), 9);
}

#[tokio::test]
async fn reader_acks_the_page_delivered_cursor() {
    let (client, transport) = client(vec![
        (
            200,
            json!({"session_epoch": 2, "delivered_cursor": null, "acknowledged_cursor": null}),
        ),
        (
            200,
            json!({
                "records": [record_json(b"k", b"v")],
                "delivered_cursor": "page-cursor",
                "acknowledged_cursor": "ack-0"
            }),
        ),
        (200, json!({"acknowledged_cursor": "page-cursor"})),
    ]);
    let mut reader = open_reader(&client, "orders.rd").await;
    let page = reader.fetch(16).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].event_time_ns, 1_700_000_000_000_000_001);
    assert_eq!(page.delivered_cursor.as_deref(), Some("page-cursor"));
    reader.ack().await.unwrap();
    let requests = transport.take_requests();
    // The ack commits the page's delivered cursor, never the record's.
    assert_eq!(requests[2].2["cursor"], "page-cursor");
    assert_eq!(reader.acknowledged_cursor(), Some("page-cursor"));
}

#[tokio::test]
async fn member_claim_surfaces_lease_classes() {
    let (client, _transport) = client(vec![(409, json!({"error": "held", "code": "lease.busy"}))]);
    let member = Member::for_testing(client, "orders.flow", Uuid::new_v4(), 1);
    assert_eq!(member.claim().await.unwrap_err(), SdkError::LeaseBusy);
}

#[tokio::test]
async fn member_claim_decodes_the_granted_lease() {
    let (client, _transport) = client(vec![(
        200,
        json!({
            "member_epoch": 1,
            "lease": {
                "work_id": Uuid::nil().to_string(),
                "member_id": Uuid::nil().to_string(),
                "member_epoch": 1,
                "lease_epoch": 4,
                "expires_at_tick": 130
            }
        }),
    )]);
    let member = Member::for_testing(client, "orders.flow", Uuid::new_v4(), 1);
    let work = member.claim().await.unwrap();
    assert_eq!(work.work_id, Uuid::nil());
    assert_eq!(work.lease_epoch, 4);
    assert_eq!(work.expires_at_tick, Some(130));
}

#[tokio::test]
async fn member_ack_carries_fetch_evidence_verbatim() {
    let (client, transport) = client(vec![(200, json!({}))]);
    let member = Member::for_testing(client, "orders.flow", Uuid::new_v4(), 1);
    let work = ClaimedWork {
        work_id: Uuid::new_v4(),
        lease_epoch: 4,
        expires_at_tick: Some(130),
    };
    let page = WorkPage {
        records: Vec::new(),
        cursor: Some("frontier-9".to_owned()),
        positions: json!({"9f5c": "pos-3"}),
        delivery_attempt: 4,
        next_delivery_ns: None,
    };
    member.ack(&work, &page).await.unwrap();
    let requests = transport.take_requests();
    let body = &requests[0].2;
    assert_eq!(body["cursor"], "frontier-9");
    assert_eq!(body["positions"], json!({"9f5c": "pos-3"}));
    assert_eq!(body["lease_epoch"], 4);
    assert!(body.get("request_id").is_some());
}

#[tokio::test]
async fn put_row_carries_mutation_id_and_typed_named_postings() {
    let (client, transport) = client(vec![(
        200,
        json!({
            "store_id": Uuid::nil().to_string(),
            "mutation_id": Uuid::nil().to_string(),
            "sequence": 3,
            "deduplicated": false
        }),
    )]);
    let store = client.state_store("crm.accounts");
    let outcome = store
        .put_row(
            Uuid::new_v4(),
            b"acct-1",
            b"{\"tier\":\"gold\"}",
            &[(
                "region".to_owned(),
                vec![TypedValue::Text("emea".to_owned())],
            )],
        )
        .await
        .unwrap();
    assert_eq!(outcome.sequence, 3);
    let requests = transport.take_requests();
    let body = &requests[0].2;
    assert_eq!(requests[0].1, "/v1/state-stores/mutate");
    assert_eq!(body["op"]["kind"], "put_row");
    assert_eq!(body["op"]["named"][0]["values"][0]["kind"], "text");
    assert!(body.get("mutation_id").is_some());
}

#[tokio::test]
async fn get_by_sends_typed_values_and_decodes_freshness() {
    let (client, transport) = client(vec![(
        200,
        json!({
            "store_id": Uuid::nil().to_string(),
            "rows": [{"key_base64": B64.encode(b"k"), "payload_base64": B64.encode(b"p")}],
            "applied_seq": 41,
            "committed_seq": 43,
            "behind": true,
            "served_by": "node-2"
        }),
    )]);
    let page = client
        .state_store("crm.accounts")
        .get_by(
            "region",
            &[TypedValue::Text("emea".to_owned())],
            Some(10),
            true,
        )
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].payload, b"p");
    assert!(page.freshness.behind);
    assert_eq!(page.freshness.applied_seq, 41);
    let requests = transport.take_requests();
    let body = &requests[0].2;
    assert_eq!(body["named_key"], "region");
    assert_eq!(body["values"][0]["kind"], "text");
    assert_eq!(body["strict"], true);
    assert_eq!(body["limit"], 10);
}

#[tokio::test]
async fn index_range_query_flattens_mode_and_bounds() {
    let (client, transport) = client(vec![(
        200,
        json!({
            "index": "crm.by_region",
            "index_id": Uuid::nil().to_string(),
            "generation": 2,
            "rows": [],
            "applied_cursor": null,
            "target_cursor": null,
            "behind": false,
            "truncated": false,
            "served_by": "node-1"
        }),
    )]);
    client
        .query_index(
            "crm.by_region",
            IndexQuery::Range {
                leading: vec![TypedValue::Text("emea".to_owned())],
                lower: IndexBound {
                    value: TypedValue::I64(0),
                    inclusive: true,
                },
                upper: IndexBound {
                    value: TypedValue::I64(99),
                    inclusive: false,
                },
            },
            Some(50),
            true,
        )
        .await
        .unwrap();
    let requests = transport.take_requests();
    let body = &requests[0].2;
    assert_eq!(body["mode"], "range");
    assert_eq!(body["leading"][0]["value"], "emea");
    assert_eq!(body["lower"]["inclusive"], true);
    assert_eq!(body["upper"]["inclusive"], false);
    assert_eq!(body["strict"], true);
}

#[tokio::test]
async fn index_behind_maps_to_a_retryable_typed_error() {
    let (client, _transport) = client(vec![(
        409,
        json!({"error": "freshness", "code": "index_behind"}),
    )]);
    let error = client
        .query_index("crm.by_region", IndexQuery::Exact(vec![]), None, true)
        .await
        .unwrap_err();
    assert_eq!(error, SdkError::IndexBehind);
    assert!(error.retryable());
}

#[tokio::test]
async fn temporary_fetch_carries_flags_and_decimal_nanos() {
    let (client, transport) = client(vec![(200, json!({"records": [], "next_cursor": "c-9"}))]);
    let page = client
        .temporary_fetch(
            "orders.events",
            TemporaryFetch {
                after: Some("resume-1".to_owned()),
                limit: 25,
                tail: true,
                wait_ms: Some(400),
                after_event_time_ns: Some(1_700_000_000_000_000_001),
                ..TemporaryFetch::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.next_cursor.as_deref(), Some("c-9"));
    let requests = transport.take_requests();
    let body = &requests[0].2;
    assert_eq!(body["tail"], true);
    assert_eq!(body["new_only"], false);
    assert_eq!(body["wait_ms"], 400);
    assert_eq!(body["after_event_time_ns"], "1700000000000000001");
}

#[tokio::test]
async fn records_passes_the_opaque_cursor_verbatim() {
    let (client, transport) = client(vec![(200, json!([record_json(b"k", b"v")]))]);
    let records = client
        .records("orders.events", Some("opaque+cursor/bytes="), 10)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let requests = transport.take_requests();
    assert_eq!(requests[0].0, "GET");
    assert!(requests[0].1.contains("after=opaque%2Bcursor%2Fbytes%3D"));
}

#[tokio::test]
async fn pipe_facade_wires_lifecycle_and_decodes_catalog_rows() {
    let subscription_id = Uuid::new_v4().to_string();
    let output_feed_id = Uuid::new_v4().to_string();
    let row = json!({
        "name": "orders.relay",
        "stage": "paused",
        "subscription_id": subscription_id,
        "operation": { "operation": "forward" },
        "output_feed_id": output_feed_id,
        "output_store_id": null,
        "status": "active",
        "created_at_ns": 42
    });
    let (client, transport) = client(vec![
        (200, json!({"results": [{"data": row}]})),
        (200, json!({"results": [{"data": [row]}]})),
        (200, json!({"results": [{}]})),
        (200, json!({"results": [{}]})),
        (200, json!({"results": [{}]})),
    ]);
    // describe decodes the catalog row verbatim.
    let described = client.pipe("orders.relay").describe().await.unwrap();
    assert_eq!(described.name, "orders.relay");
    assert_eq!(described.stage, "paused");
    assert_eq!(described.subscription_id.to_string(), subscription_id);
    assert_eq!(described.operation["operation"], "forward");
    assert_eq!(
        described.output_feed_id.unwrap().to_string(),
        output_feed_id
    );
    assert_eq!(described.output_store_id, None);
    // pipes lists the same row; lifecycle commands post the typed verbs.
    let listed = client.pipes().await.unwrap();
    assert_eq!(listed.len(), 1);
    client.pipe("orders.relay").pause().await.unwrap();
    client.pipe("orders.relay").resume().await.unwrap();
    client.pipe("orders.relay").drop().await.unwrap();
    let requests = transport.take_requests();
    let command = |index: usize| requests[index].2["commands"][0].clone();
    assert_eq!(command(0)["command"], "describe");
    assert_eq!(command(0)["kind"], "pipe");
    assert_eq!(command(0)["name"], "orders.relay");
    assert_eq!(command(1)["kind"], "pipes");
    assert_eq!(command(2)["command"], "pause_pipe");
    assert_eq!(command(3)["command"], "resume_pipe");
    assert_eq!(command(4)["command"], "drop");
    assert_eq!(command(4)["kind"], "pipe");
    assert!(requests
        .iter()
        .all(|request| request.1 == "/v1/admin/commands"));
}

// ---- codec layer ------------------------------------------------------------

#[test]
fn utf8_codec_round_trips_and_rejects_invalid_bytes() {
    let codec = Codec::<String>::utf8();
    let bytes = codec.encode(&"order-1".to_owned()).unwrap();
    assert_eq!(codec.decode(&bytes).unwrap(), "order-1");
    assert_eq!(codec.name(), "utf8");
    let error = codec.decode(&[0xFF, 0xFE]).unwrap_err();
    assert!(matches!(error, SdkError::Codec(_)), "{error:?}");
}

#[test]
fn bytes_codec_passes_through() {
    let codec = Codec::<Vec<u8>>::bytes();
    let raw = vec![0x00, 0xFF, 0x10];
    assert_eq!(codec.decode(&codec.encode(&raw).unwrap()).unwrap(), raw);
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Order {
    id: String,
    total: i64,
}

#[test]
fn json_codec_round_trips_structs() {
    let codec = Codec::<Order>::json();
    let order = Order {
        id: "o-1".to_owned(),
        total: 4200,
    };
    let bytes = codec.encode(&order).unwrap();
    assert_eq!(codec.decode(&bytes).unwrap(), order);
}

#[test]
fn custom_codec_maps_failures_to_codec_errors() {
    let codec = Codec::<u64>::custom(
        "u64-text",
        |value| Ok(value.to_string().into_bytes()),
        |bytes| {
            String::from_utf8(bytes.to_vec())
                .ok()
                .and_then(|text| text.parse::<u64>().ok())
                .ok_or_else(|| SdkError::Codec("not a u64".to_owned()))
        },
    );
    assert_eq!(codec.decode(&codec.encode(&7_u64).unwrap()).unwrap(), 7);
    assert!(matches!(codec.decode(b"nope"), Err(SdkError::Codec(_))));
}

// ---- typed feed facade ------------------------------------------------------

#[tokio::test]
async fn feed_writer_declares_and_encodes_through_codecs() {
    let (client, transport) = client(vec![
        (200, json!({"results": [{}]})), // CREATE WRITER
        (200, json!({"results": [{"data": {"session_epoch": 7}}]})),
        (
            200,
            json!({
                "message_id": Uuid::nil().to_string(),
                "cursor": "cur-1",
                "deduplicated": false
            }),
        ),
    ]);
    let feed = client.feed_with("orders.events", Codec::utf8(), Codec::<Order>::json());
    let writer = feed.writer("checkout").await.unwrap();
    let order = Order {
        id: "o-1".to_owned(),
        total: 4200,
    };
    writer.append(&"order-1".to_owned(), &order).await.unwrap();
    let requests = transport.take_requests();
    // CREATE WRITER ran over WCL, session opened, then one typed append.
    assert_eq!(requests[0].1, "/v1/admin/wcl");
    assert_eq!(requests[1].1, "/v1/admin/wcl");
    let body = &requests[2].2;
    assert_eq!(body["key_base64"], B64.encode(b"order-1"));
    let payload: Order = serde_json::from_slice(
        &B64.decode(body["payload_base64"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(payload, order);
}

#[tokio::test]
async fn feed_writer_append_event_pins_identity_time_and_metadata() {
    let (client, transport) = client(vec![
        (200, json!({"results": [{}]})),
        (200, json!({"results": [{"data": {"session_epoch": 1}}]})),
        (
            200,
            json!({
                "message_id": Uuid::nil().to_string(),
                "cursor": "c",
                "deduplicated": true
            }),
        ),
    ]);
    let feed = client.feed("orders.events");
    let writer = feed.writer("checkout").await.unwrap();
    let request_id = Uuid::new_v4();
    let outcome = writer
        .append_event(
            FeedEvent::new("order-9".to_owned(), "created".to_owned())
                .with_request_id(request_id)
                .with_event_time_ns(1_700_000_000_000_000_001),
        )
        .await
        .unwrap();
    assert!(outcome.deduplicated);
    let requests = transport.take_requests();
    let body = &requests[2].2;
    assert_eq!(body["request_id"], request_id.to_string());
    assert_eq!(body["event_time_ns"], "1700000000000000001");
}

#[tokio::test]
async fn feed_writer_keyless_event_sends_an_empty_key() {
    let (client, transport) = client(vec![
        (200, json!({"results": [{}]})),
        (200, json!({"results": [{"data": {"session_epoch": 1}}]})),
        (
            200,
            json!({
                "message_id": Uuid::nil().to_string(),
                "cursor": "c",
                "deduplicated": false
            }),
        ),
    ]);
    let feed = client.feed("orders.events");
    let writer = feed.writer("checkout").await.unwrap();
    writer
        .append_event(FeedEvent::keyless("heartbeat".to_owned()))
        .await
        .unwrap();
    let requests = transport.take_requests();
    assert_eq!(requests[2].2["key_base64"], "");
}

#[tokio::test]
async fn feed_writer_batch_encodes_each_record() {
    let (client, transport) = client(vec![
        (200, json!({"results": [{}]})),
        (200, json!({"results": [{"data": {"session_epoch": 5}}]})),
        (
            200,
            json!({"results": [
                {"message_id": Uuid::nil().to_string(), "cursor": "c1"},
                {"message_id": Uuid::nil().to_string(), "cursor": "c2"}
            ]}),
        ),
    ]);
    let feed = client.feed("orders.events");
    let writer = feed.writer("checkout").await.unwrap();
    let outcomes = writer
        .append_batch(&[
            FeedEvent::new("a".to_owned(), "1".to_owned()),
            FeedEvent::new("b".to_owned(), "2".to_owned()),
        ])
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    let requests = transport.take_requests();
    let body = &requests[2].2;
    assert_eq!(requests[2].1, "/v1/writers/append-batch");
    assert_eq!(body["records"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn feed_writer_propagates_a_non_exists_create_failure() {
    let (client, _transport) = client(vec![(400, json!({"error": "invalid name: BAD NAME"}))]);
    let feed = client.feed("orders.events");
    let error = feed.writer("bad").await.err().unwrap();
    assert!(matches!(error, SdkError::Refused(_)), "{error:?}");
}

#[tokio::test]
async fn feed_writer_tolerates_an_existing_declaration() {
    let (client, _transport) = client(vec![
        (400, json!({"error": "resource already exists: wr"})),
        (200, json!({"results": [{"data": {"session_epoch": 2}}]})),
    ]);
    let feed = client.feed("orders.events");
    let writer = feed.writer("checkout").await.unwrap();
    assert_eq!(writer.session_epoch(), 2);
}

#[tokio::test]
async fn feed_reader_decodes_pages_and_acks_delivered_cursor() {
    let (client, transport) = client(vec![
        (200, json!({"results": [{}]})), // CREATE READER
        (
            200,
            json!({"session_epoch": 4, "delivered_cursor": null, "acknowledged_cursor": null}),
        ),
        (
            200,
            json!({
                "records": [
                    record_json(b"order-1", br#"{"id":"o-1","total":42}"#),
                    {
                        "message_id": Uuid::nil().to_string(),
                        "key_base64": "",
                        "payload_base64": B64.encode(b"ping"),
                        "metadata_base64": {},
                        "event_time_ns": "5", "ingest_time_ns": "6",
                        "cursor": "k2"
                    }
                ],
                "delivered_cursor": "page-9",
                "acknowledged_cursor": "ack-8",
                "recommended_capacity": 32
            }),
        ),
        (200, json!({"acknowledged_cursor": "page-9"})),
    ]);
    let feed = client.feed("orders.events");
    let mut reader = feed.reader("audit", 64).await.unwrap();
    let page = reader.fetch(0).await.unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].key.as_deref(), Some("order-1"));
    // Empty key bytes decode as a keyless record.
    assert_eq!(page.records[1].key, None);
    assert_eq!(page.delivered_cursor.as_deref(), Some("page-9"));
    assert_eq!(page.recommended_capacity, Some(32));
    reader.ack().await.unwrap();
    let requests = transport.take_requests();
    assert_eq!(requests[3].1, "/v1/readers/ack");
    assert_eq!(requests[3].2["cursor"], "page-9");
    assert_eq!(reader.acknowledged_cursor(), Some("page-9"));
    reader.close().await.ok();
}

#[tokio::test]
async fn feed_reader_json_decodes_typed_values() {
    let (client, _transport) = client(vec![
        (200, json!({"results": [{}]})),
        (200, json!({"session_epoch": 1})),
        (
            200,
            json!({
                "records": [record_json(b"order-1", br#"{"id":"o-1","total":42}"#)],
                "delivered_cursor": "p"
            }),
        ),
    ]);
    let feed = client.feed_with("orders.events", Codec::utf8(), Codec::<Order>::json());
    let mut reader = feed.reader("audit", 64).await.unwrap();
    let page = reader.fetch(0).await.unwrap();
    assert_eq!(
        page.records[0].value,
        Order {
            id: "o-1".to_owned(),
            total: 42
        }
    );
}

#[tokio::test]
async fn feed_records_decode_and_preserve_envelope_fields() {
    let (client, _transport) = client(vec![(
        200,
        json!([{
            "message_id": Uuid::nil().to_string(),
            "key_base64": B64.encode(b"k"),
            "payload_base64": B64.encode(b"v"),
            "metadata_base64": {"source": B64.encode(b"test")},
            "event_time_ns": "1700000000000000001",
            "ingest_time_ns": "1700000000000000002",
            "cursor": "opaque-cursor"
        }]),
    )]);
    let feed = client.feed("orders.events");
    let records = feed.records(None, 10).await.unwrap();
    assert_eq!(records.len(), 1);
    let message = &records[0];
    assert_eq!(message.key.as_deref(), Some("k"));
    assert_eq!(message.value, "v");
    assert_eq!(message.cursor.as_deref(), Some("opaque-cursor"));
    assert_eq!(message.event_time_ns, 1_700_000_000_000_000_001);
    assert_eq!(message.metadata_text("source").as_deref(), Some("test"));
}

#[tokio::test]
async fn feed_load_reads_from_the_beginning() {
    let (client, transport) = client(vec![(200, json!([record_json(b"k", b"v")]))]);
    let feed = client.feed("orders.events");
    let records = feed.load(10).await.unwrap();
    assert_eq!(records.len(), 1);
    let requests = transport.take_requests();
    // No `after` parameter — the server starts at the first record.
    assert!(!requests[0].1.contains("after="));
}

#[tokio::test]
async fn feed_fetch_decodes_temporary_pages() {
    let (client, _transport) = client(vec![(
        200,
        json!({
            "records": [record_json(b"k", b"v")],
            "next_cursor": "next-3"
        }),
    )]);
    let feed = client.feed("orders.events");
    let page = feed
        .fetch(TemporaryFetch {
            tail: true,
            wait_ms: Some(100),
            limit: 5,
            ..TemporaryFetch::default()
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.next_cursor.as_deref(), Some("next-3"));
}

#[tokio::test]
async fn feed_view_rebinds_codecs_without_a_wire_call() {
    let (client, transport) = client(vec![]);
    let feed = client.feed("orders.events");
    let rebound: Feed<String, Order> = feed.view(Codec::utf8(), Codec::json());
    assert_eq!(rebound.name(), "orders.events");
    assert!(transport.take_requests().is_empty());
}

#[tokio::test]
async fn feed_records_surface_codec_failures_as_codec_errors() {
    let (client, _transport) = client(vec![(
        200,
        json!([{
            "key_base64": B64.encode([0xFF, 0xFE]),
            "payload_base64": B64.encode(b"v"),
            "metadata_base64": {},
            "event_time_ns": "1", "ingest_time_ns": "2"
        }]),
    )]);
    let feed = client.feed("orders.events");
    let error = feed.records(None, 10).await.unwrap_err();
    assert!(matches!(error, SdkError::Codec(_)), "{error:?}");
}

#[tokio::test]
async fn subscribe_delivers_batches_and_acks_progress() {
    let (client, transport) = client(vec![
        (200, json!({"results": [{}]})), // CREATE READER
        (200, json!({"session_epoch": 6})),
        (
            200,
            json!({
                "records": [record_json(b"k", b"v")],
                "delivered_cursor": "d-1",
                "acknowledged_cursor": "a-0"
            }),
        ),
        (200, json!({"acknowledged_cursor": "d-1"})),
    ]);
    let feed = client.feed("orders.events");
    let (tx, rx) = tokio::sync::oneshot::channel::<usize>();
    let tx = std::sync::Mutex::new(Some(tx));
    let subscription = feed
        .subscribe("audit", 64, 8, move |batch| {
            let tx = tx.lock().unwrap().take();
            async move {
                if let Some(tx) = tx {
                    let _ = tx.send(batch.len());
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
        .await
        .expect("subscription delivers the page")
        .unwrap();
    assert_eq!(delivered, 1);
    subscription.stop();
    // The next poll may surface a terminal error before the abort
    // lands — either outcome is honest; the delivery+ack assertions
    // above are the contract.
    let _ = subscription.wait().await;
    let requests = transport.take_requests();
    assert!(requests
        .iter()
        .any(|request| request.1 == "/v1/readers/ack"));
}

// ---- typed store facade -----------------------------------------------------

fn mutation_reply() -> Value {
    json!({
        "store_id": Uuid::nil().to_string(),
        "mutation_id": Uuid::nil().to_string(),
        "sequence": 3,
        "deduplicated": false
    })
}

#[tokio::test]
async fn store_put_encodes_key_and_payload_through_codecs() {
    let (client, transport) = client(vec![(200, mutation_reply())]);
    let store = client.store_with("crm.accounts", Codec::utf8(), Codec::<Order>::json());
    store
        .put(
            Uuid::new_v4(),
            &"acct-1".to_owned(),
            &Order {
                id: "o-1".to_owned(),
                total: 5,
            },
        )
        .await
        .unwrap();
    let requests = transport.take_requests();
    let body = &requests[0].2;
    assert_eq!(body["op"]["key_base64"], B64.encode(b"acct-1"));
    let payload: Order = serde_json::from_slice(
        &B64.decode(body["op"]["payload_base64"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(payload.id, "o-1");
}

#[tokio::test]
async fn store_get_decodes_rows_and_passes_freshness_through() {
    let (client, _transport) = client(vec![(
        200,
        json!({
            "found": true,
            "payload_base64": B64.encode(b"p"),
            "applied_seq": 9,
            "committed_seq": 9,
            "behind": false,
            "served_by": "node-3"
        }),
    )]);
    let store = client.store("crm.accounts");
    let lookup = store.get(&"acct-1".to_owned(), true).await.unwrap();
    let row = lookup.row.expect("row found");
    assert_eq!(row.key, "acct-1");
    assert_eq!(row.value, "p");
    assert_eq!(lookup.freshness.applied_seq, 9);
    assert_eq!(lookup.freshness.served_by, "node-3");
}

#[tokio::test]
async fn store_get_returns_none_for_a_miss() {
    let (client, _transport) = client(vec![(
        200,
        json!({"found": false, "applied_seq": 9, "committed_seq": 9}),
    )]);
    let store = client.store("crm.accounts");
    let lookup = store.get(&"absent".to_owned(), false).await.unwrap();
    assert!(lookup.row.is_none());
}

#[tokio::test]
async fn store_iterate_decodes_rows_and_continuation_key() {
    let (client, transport) = client(vec![(
        200,
        json!({
            "rows": [{"key_base64": B64.encode(b"k1"), "payload_base64": B64.encode(b"v1")}],
            "next_key_base64": B64.encode(b"k2"),
            "pinned_seq": 44,
            "served_by": "node-1"
        }),
    )]);
    let store = client.store("crm.accounts");
    let page = store
        .iterate(Some(&"k0".to_owned()), Some(50))
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].key, "k1");
    assert_eq!(page.next_key.as_deref(), Some("k2"));
    assert_eq!(page.pinned_seq, 44);
    let requests = transport.take_requests();
    assert_eq!(requests[0].2["after_key_base64"], B64.encode(b"k0"));
}

#[tokio::test]
async fn store_delete_clear_and_get_by_stay_typed() {
    let (client, _transport) = client(vec![
        (200, mutation_reply()),
        (200, mutation_reply()),
        (
            200,
            json!({
                "rows": [{"key_base64": B64.encode(b"k"), "payload_base64": B64.encode(b"p")}],
                "applied_seq": 1, "committed_seq": 1, "behind": false, "served_by": "n"
            }),
        ),
    ]);
    let store = client.store("crm.accounts");
    store.delete(Uuid::new_v4(), &"k".to_owned()).await.unwrap();
    store.clear(Uuid::new_v4()).await.unwrap();
    let page = store
        .get_by(
            "region",
            &[TypedValue::Text("emea".to_owned())],
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(page.rows[0].key, "k");
}

#[tokio::test]
async fn store_view_rebinds_codecs_without_a_wire_call() {
    let (client, transport) = client(vec![]);
    let store = client.store("crm.accounts");
    let rebound: Store<String, Order> = store.view(Codec::utf8(), Codec::json());
    assert_eq!(rebound.name(), "crm.accounts");
    assert!(transport.take_requests().is_empty());
}

#[tokio::test]
async fn store_sync_and_rebuild_pass_through() {
    let (client, _transport) = client(vec![
        (
            200,
            json!({"applied": 12, "frontier": "f-9", "complete": true}),
        ),
        (
            200,
            json!({"applied": 4, "frontier": "f-4", "complete": false}),
        ),
    ]);
    let store = client.store("crm.accounts");
    let sync = store.sync().await.unwrap();
    assert!(sync.complete);
    let rebuild = store.rebuild(Some(Uuid::nil())).await.unwrap();
    assert_eq!(rebuild.applied, 4);
    assert!(!rebuild.complete);
}

// ---- demand observability ---------------------------------------------------

#[tokio::test]
async fn demand_reports_decode_rows_and_domain_rollups() {
    let (client, transport) = client(vec![
        (
            200,
            json!({
                "feeds": [{
                    "feed": "orders.events",
                    "domain": "orders",
                    "appended_records": 1500,
                    "appended_bytes": 2048000,
                    "read_pages": 42
                }],
                "domains": {"orders": {"feeds": 1, "appended_records": 1500}}
            }),
        ),
        (
            200,
            json!({
                "hot_keys": [{
                    "feed": "orders.events",
                    "share_bps": 9000,
                    "window_records": 900,
                    "window_bytes": 12000
                }]
            }),
        ),
    ]);
    let report = client.demand_feeds().await.unwrap();
    assert_eq!(report.rows.len(), 1);
    assert_eq!(report.rows[0].name, "orders.events");
    assert_eq!(report.rows[0].domain.as_deref(), Some("orders"));
    assert_eq!(report.rows[0].counter("appended_records"), Some(1500));
    assert_eq!(report.domains["orders"]["feeds"], 1);
    let hot = client.hot_keys().await.unwrap();
    assert_eq!(hot.len(), 1);
    assert_eq!(hot[0].feed(), Some("orders.events"));
    assert_eq!(hot[0].share_bps(), Some(9000));
    let requests = transport.take_requests();
    assert_eq!(requests[0].1, "/v1/admin/demand/feeds");
    assert_eq!(requests[1].1, "/v1/admin/demand/hot-keys");
}

#[tokio::test]
async fn demand_endpoints_cover_every_resource_kind() {
    let empty = |key: &str| (200, json!({key: [], "domains": {}}));
    let (client, transport) = client(vec![
        empty("readers"),
        empty("subscriptions"),
        empty("indexes"),
        empty("pipes"),
    ]);
    assert!(client.demand_readers().await.unwrap().rows.is_empty());
    assert!(client.demand_subscriptions().await.unwrap().rows.is_empty());
    assert!(client.demand_indexes().await.unwrap().rows.is_empty());
    assert!(client.demand_pipes().await.unwrap().rows.is_empty());
    let requests = transport.take_requests();
    assert_eq!(requests[0].1, "/v1/admin/demand/readers");
    assert_eq!(requests[1].1, "/v1/admin/demand/subscriptions");
    assert_eq!(requests[2].1, "/v1/admin/demand/indexes");
    assert_eq!(requests[3].1, "/v1/admin/demand/pipes");
}

/// Append-batch transport that refuses bodies over `limit` records with
/// 413 and echoes each record's `request_id` back as the outcome cursor
/// so split order is observable.
struct LimitTransport {
    limit: usize,
    calls: Mutex<Vec<Vec<String>>>,
    binary_calls: Mutex<Vec<Vec<u8>>>,
}

impl LimitTransport {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            calls: Mutex::new(Vec::new()),
            binary_calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Transport for LimitTransport {
    async fn request(
        &self,
        _base: &str,
        _method: &str,
        path: &str,
        body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        if path != "/v1/writers/append-batch" {
            return Ok(TransportReply {
                status: 200,
                body: json!({"results": [{"data": {"session_epoch": 7}}]}),
            });
        }
        let records = body
            .and_then(|body| body.get("records"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.calls.lock().unwrap().push(
            records
                .iter()
                .filter_map(|record| {
                    record
                        .get("request_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .collect(),
        );
        if records.len() > self.limit {
            return Ok(TransportReply {
                status: 413,
                body: json!({"error": "body too large"}),
            });
        }
        let results: Vec<Value> = records
            .iter()
            .map(|record| {
                json!({
                    "message_id": Uuid::nil().to_string(),
                    "cursor": record["request_id"].as_str().unwrap_or_default(),
                    "deduplicated": false,
                })
            })
            .collect();
        Ok(TransportReply {
            status: 200,
            body: json!({ "results": results }),
        })
    }

    async fn request_binary_body(
        &self,
        _base: &str,
        path: &str,
        body: &[u8],
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        assert_eq!(path, "/v1/writers/append-batch");
        self.binary_calls.lock().unwrap().push(body.to_vec());
        let count = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
        let results: Vec<Value> = (0..count)
            .map(|index| {
                json!({
                    "message_id": Uuid::nil().to_string(),
                    "cursor": format!("c{index}"),
                    "deduplicated": false,
                })
            })
            .collect();
        Ok(TransportReply {
            status: 200,
            body: json!({ "results": results }),
        })
    }
}

fn limit_client(limit: usize) -> (Client, Arc<LimitTransport>) {
    let transport = Arc::new(LimitTransport::new(limit));
    let client = Client::new(
        transport.clone(),
        vec!["http://node1".to_owned()],
        "test-key",
    )
    .expect("test client");
    (client, transport)
}

fn append_item(request_id: Uuid, metadata: BTreeMap<String, Vec<u8>>) -> AppendItem {
    AppendItem {
        request_id,
        key: b"k".to_vec(),
        payload: b"v".to_vec(),
        event_time_ns: 1_700_000_000_000_000_001,
        metadata,
    }
}

#[tokio::test]
async fn append_batch_splits_on_payload_too_large_and_preserves_order() {
    let (client, transport) = limit_client(2);
    let writer = open_writer(&client, "orders.wr").await;
    let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    let records: Vec<(Uuid, Vec<u8>, Vec<u8>, i64)> = ids
        .iter()
        .map(|id| (*id, b"k".to_vec(), b"v".to_vec(), 1))
        .collect();
    let outcomes = writer.append_batch(&records).await.unwrap();
    assert_eq!(outcomes.len(), 4);
    let cursors: Vec<String> = outcomes
        .iter()
        .map(|outcome| outcome.cursor.clone())
        .collect();
    assert_eq!(cursors, ids.iter().map(Uuid::to_string).collect::<Vec<_>>());
    // One refused 4-record call, then two 2-record halves in order.
    let calls = transport.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].len(), 4);
    assert_eq!(
        calls[1],
        ids[..2].iter().map(Uuid::to_string).collect::<Vec<_>>()
    );
    assert_eq!(
        calls[2],
        ids[2..].iter().map(Uuid::to_string).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn append_batch_single_record_surfaces_payload_too_large() {
    let (client, transport) = limit_client(0);
    let writer = open_writer(&client, "orders.wr").await;
    let error = writer
        .append_batch(&[(Uuid::new_v4(), b"k".to_vec(), b"v".to_vec(), 1)])
        .await
        .unwrap_err();
    assert!(matches!(error, SdkError::PayloadTooLarge(_)));
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn append_items_json_carries_metadata_and_compression_flag() {
    let (client, transport) = client(vec![
        (200, json!({"results": [{"data": {"session_epoch": 7}}]})),
        (
            200,
            json!({"results": [{
                "message_id": Uuid::nil().to_string(),
                "cursor": "c",
                "deduplicated": false,
            }]}),
        ),
    ]);
    let client = client.use_compression(true);
    let writer = open_writer(&client, "orders.wr").await;
    writer
        .append_items(&[append_item(
            Uuid::new_v4(),
            BTreeMap::from([("source".to_owned(), b"conformance".to_vec())]),
        )])
        .await
        .unwrap();
    let requests = transport.take_requests();
    let (_, _, body) = &requests[1];
    let record = &body["records"][0];
    assert_eq!(record["use_compression"], true);
    assert_eq!(
        record["metadata_base64"]["source"],
        B64.encode(b"conformance")
    );
}

#[tokio::test]
async fn append_items_binary_frames_carry_metadata_and_compression_flag() {
    let (client, transport) = limit_client(100);
    let client = client.use_compression(true);
    let writer = open_writer(&client, "orders.wr").await.binary_appends();
    writer
        .append_items(&[append_item(
            Uuid::new_v4(),
            BTreeMap::from([("source".to_owned(), b"conformance".to_vec())]),
        )])
        .await
        .unwrap();
    let calls = transport.binary_calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    let page = &calls[0];
    assert_eq!(u32::from_be_bytes(page[..4].try_into().unwrap()), 1);
    // Frame: u32be body_len | body | u32be crc.
    let body_len = u32::from_be_bytes(page[4..8].try_into().unwrap()) as usize;
    let body = &page[8..8 + body_len];
    assert_eq!(&body[..4], b"FSA1");
    let writer_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
    let mut cursor = 6 + writer_len + 16 + 8;
    let flags = body[cursor];
    assert_eq!(flags & 0x02, 0x02, "compression flag bit must be set");
    assert_eq!(flags & 0x01, 0x01, "event_time flag must be set");
    cursor += 1 + 8; // flags + event_time_ns
    let key_len = u32::from_be_bytes(body[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4 + key_len;
    let payload_len = u32::from_be_bytes(body[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4 + payload_len;
    let metadata_count = u16::from_be_bytes(body[cursor..cursor + 2].try_into().unwrap()) as usize;
    cursor += 2;
    assert_eq!(metadata_count, 1);
    let name_len = u16::from_be_bytes(body[cursor..cursor + 2].try_into().unwrap()) as usize;
    let value_len = u32::from_be_bytes(body[cursor + 2..cursor + 6].try_into().unwrap()) as usize;
    cursor += 6;
    assert_eq!(&body[cursor..cursor + name_len], b"source");
    cursor += name_len;
    assert_eq!(&body[cursor..cursor + value_len], b"conformance");
}

/// Pump-policy transport: append-batch replies are scripted by
/// `responder`, every other path opens the session.
struct PumpTransport {
    calls: Mutex<usize>,
    responder: fn() -> Result<TransportReply, SdkError>,
}

#[async_trait]
impl Transport for PumpTransport {
    async fn request(
        &self,
        _base: &str,
        _method: &str,
        path: &str,
        _body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        if path == "/v1/writers/append-batch" {
            *self.calls.lock().unwrap() += 1;
            return (self.responder)();
        }
        Ok(TransportReply {
            status: 200,
            body: json!({"results": [{"data": {"session_epoch": 7}}]}),
        })
    }
}

/// Counts concurrent append-batch calls: with 50 ms service and a full
/// queue the pump must overlap batches up to max_in_flight.
struct ConcurrencyTransport {
    in_flight: Mutex<usize>,
    high_water: Mutex<usize>,
    completed: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl Transport for ConcurrencyTransport {
    async fn request(
        &self,
        _base: &str,
        _method: &str,
        path: &str,
        body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        if path == "/v1/writers/append-batch" {
            {
                let mut current = self.in_flight.lock().unwrap();
                *current += 1;
                let mut high = self.high_water.lock().unwrap();
                *high = (*high).max(*current);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            *self.in_flight.lock().unwrap() -= 1;
            let records = body.and_then(|body| body.get("records")?.as_array());
            let cursors: Vec<String> = records
                .map(|records| {
                    records
                        .iter()
                        .filter_map(|r| {
                            r.get("key_base64")
                                .or_else(|| r.get("cursor"))?
                                .as_str()
                                .map(str::to_owned)
                        })
                        .collect()
                })
                .unwrap_or_default();
            self.completed.lock().unwrap().push(cursors);
            let count = records.map(|records| records.len()).unwrap_or(0);
            return Ok(TransportReply {
                status: 200,
                body: json!({
                    "results": (0..count)
                        .map(|_| json!({
                            "message_id": Uuid::new_v4().to_string(),
                            "cursor": "c-1",
                            // The server's pressure shed: observe() must
                            // not let it collapse concurrency to one
                            // round — a full queue still overlaps.
                            "feedback": {"recommended_batch_count": 16},
                        }))
                        .collect::<Vec<_>>()
                }),
            });
        }
        Ok(TransportReply {
            status: 200,
            body: json!({"results": [{"data": {"session_epoch": 7}}]}),
        })
    }
}

#[tokio::test]
async fn pump_overlaps_batches_up_to_max_in_flight() {
    let transport = Arc::new(ConcurrencyTransport {
        in_flight: Mutex::new(0),
        high_water: Mutex::new(0),
        completed: Mutex::new(Vec::new()),
    });
    let client = Client::new(
        transport.clone(),
        vec!["http://node1".to_owned()],
        "test-key",
    )
    .unwrap();
    let writer = open_writer(&client, "orders.wr").await;
    let pumped = writer.batched(Batching {
        linger: std::time::Duration::from_millis(5),
        max_batch: 512,
        max_in_flight: 5,
    });
    let mut appends = tokio::task::JoinSet::new();
    for index in 0..4_096_u64 {
        let pumped = pumped.clone();
        appends.spawn(async move {
            pumped
                .append(
                    Uuid::new_v4(),
                    format!("key-{index:012}").as_bytes(),
                    b"v",
                    1,
                )
                .await
        });
    }
    while let Some(result) = appends.join_next().await {
        result.unwrap().unwrap();
    }
    pumped.flush().await.unwrap();
    let high_water = *transport.high_water.lock().unwrap();
    assert!(
        high_water >= 2,
        "pump ran {high_water} concurrent append-batches with a full queue and max_in_flight=5"
    );
    // Per-Key order: for one key, cursors land in append order across
    // every batch they appear in.
    let mut seen = Vec::new();
    for batch in transport.completed.lock().unwrap().iter() {
        for cursor in batch {
            if cursor.contains("key-0") || !seen.contains(cursor) {
                seen.push(cursor.clone());
            }
        }
    }
    assert!(!seen.is_empty());
}

fn batching() -> Batching {
    Batching {
        linger: std::time::Duration::from_millis(1),
        max_batch: 4,
        max_in_flight: 1,
    }
}

#[tokio::test]
async fn pump_returns_authoritative_errors_without_retry() {
    let transport = Arc::new(PumpTransport {
        calls: Mutex::new(0),
        responder: || {
            Ok(TransportReply {
                status: 409,
                body: json!({"error": "superseded", "code": "stale_epoch"}),
            })
        },
    });
    let client = Client::new(
        transport.clone(),
        vec!["http://node1".to_owned()],
        "test-key",
    )
    .unwrap();
    let writer = open_writer(&client, "orders.wr").await;
    let pumped = writer.batched(batching());
    let error = pumped
        .append(Uuid::new_v4(), b"k", b"v", 1)
        .await
        .unwrap_err();
    assert_eq!(error, SdkError::StaleEpoch);
    assert_eq!(*transport.calls.lock().unwrap(), 1);
    pumped.flush().await.unwrap();
}

#[tokio::test]
async fn pump_retries_transport_errors() {
    let transport = Arc::new(PumpTransport {
        calls: Mutex::new(0),
        responder: || Err(SdkError::Transport("connection reset".to_owned())),
    });
    let client = Client::new(
        transport.clone(),
        vec!["http://node1".to_owned()],
        "test-key",
    )
    .unwrap();
    let writer = open_writer(&client, "orders.wr").await;
    let pumped = writer.batched(batching());
    let error = pumped
        .append(Uuid::new_v4(), b"k", b"v", 1)
        .await
        .unwrap_err();
    assert!(matches!(error, SdkError::Transport(_)));
    // 5 append attempts, each re-pinned to the same single endpoint, so
    // one transport call per attempt... plus the failover probe per
    // attempt — the retry loop, not the error, drives repetition.
    assert!(*transport.calls.lock().unwrap() >= 5);
    pumped.flush().await.unwrap();
}

/// Binary-reply transport: FIFO raw pages for `request_binary` so the
/// defaulted `request_frame_page` path is exercised end to end.
type ScriptedPageReply = Result<(u16, Vec<u8>), SdkError>;

struct PageStubTransport {
    replies: Mutex<std::collections::VecDeque<ScriptedPageReply>>,
}

#[async_trait]
impl Transport for PageStubTransport {
    async fn request(
        &self,
        _base: &str,
        _method: &str,
        _path: &str,
        _body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        Ok(TransportReply {
            status: 500,
            body: Value::Null,
        })
    }

    async fn request_binary(
        &self,
        _base: &str,
        _path: &str,
        _body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportBytesReply, SdkError> {
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(SdkError::Transport("no scripted page".to_owned())))
            .map(|(status, body)| TransportBytesReply {
                status,
                body,
                headers: Vec::new(),
            })
    }
}

/// Minimal page encoder matching `application/vnd.whitewater.frames`:
/// FSR2 record bodies inside the `u32 len | body | crc32` envelope.
fn stub_frame(sequence: u64, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"FSR2");
    body.extend_from_slice(&[0u8; 16]);
    body.extend_from_slice(&[0u8; 16]);
    body.extend_from_slice(&sequence.to_be_bytes());
    body.extend_from_slice(&1i64.to_be_bytes());
    body.extend_from_slice(&2i64.to_be_bytes());
    body.extend_from_slice(&1u32.to_be_bytes());
    body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(b"k");
    body.extend_from_slice(payload);
    let mut frame = Vec::new();
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    frame
}

fn stub_page(records: &[(u64, &[u8], &str)], next_cursor: &str) -> Vec<u8> {
    let mut page = Vec::new();
    page.extend_from_slice(&(records.len() as u32).to_le_bytes());
    page.extend_from_slice(&(next_cursor.len() as u32).to_le_bytes());
    page.extend_from_slice(next_cursor.as_bytes());
    for (sequence, payload, cursor) in records {
        page.extend_from_slice(&(cursor.len() as u32).to_le_bytes());
        page.extend_from_slice(cursor.as_bytes());
        let frame = stub_frame(*sequence, payload);
        page.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        page.extend_from_slice(&frame);
    }
    page
}

#[tokio::test]
async fn pipelined_fetch_returns_pages_in_order_and_stops_on_error() {
    let pages = vec![
        Ok((200u16, stub_page(&[(1, b"a", "c1")], "n1"))),
        Ok((200u16, stub_page(&[(2, b"b", "c2")], "n2"))),
        Ok((200u16, stub_page(&[(3, b"c", "c3")], ""))),
    ];
    let transport = Arc::new(PageStubTransport {
        replies: Mutex::new(pages.into()),
    });
    let client = Client::new(
        transport.clone(),
        vec!["http://node1".to_owned()],
        "test-key",
    )
    .unwrap();
    let mut stream = client.temporary_fetch_frames_pipelined(
        "orders.events",
        TemporaryFetch {
            after: None,
            limit: 10,
            new_only: false,
            tail: false,
            wait_ms: None,
            after_event_time_ns: None,
        },
        1,
    );
    let page1 = stream.next().await.unwrap().unwrap();
    assert_eq!(page1.records.len(), 1);
    assert_eq!(page1.next_cursor.as_deref(), Some("n1"));
    let page2 = stream.next().await.unwrap().unwrap();
    assert_eq!(page2.next_cursor.as_deref(), Some("n2"));
    let page3 = stream.next().await.unwrap().unwrap();
    assert_eq!(page3.records.len(), 1);
    assert_eq!(page3.next_cursor, None);
    assert!(stream.next().await.is_none());

    // An error on a later page surfaces and ends the stream.
    let transport = Arc::new(PageStubTransport {
        replies: Mutex::new(
            vec![
                Ok((200u16, stub_page(&[(1, b"a", "c1")], "n1"))),
                Err(SdkError::Transport("dropped".to_owned())),
            ]
            .into(),
        ),
    });
    let client = Client::new(
        transport.clone(),
        vec!["http://node1".to_owned()],
        "test-key",
    )
    .unwrap();
    let mut stream = client.temporary_fetch_frames_pipelined(
        "orders.events",
        TemporaryFetch {
            after: None,
            limit: 10,
            new_only: false,
            tail: false,
            wait_ms: None,
            after_event_time_ns: None,
        },
        1,
    );
    assert!(stream.next().await.unwrap().is_ok());
    assert!(matches!(
        stream.next().await.unwrap(),
        Err(SdkError::Transport(_))
    ));
    assert!(stream.next().await.is_none());
}

/// Pump-pipeline transport: 20 ms per append-batch, records every call's
/// start instant and per-request keys so tests can assert concurrency
/// and per-Key ordering.
struct PipelineTransport {
    calls: Mutex<Vec<(std::time::Instant, Vec<Vec<u8>>)>>,
}

#[async_trait]
impl Transport for PipelineTransport {
    async fn request(
        &self,
        _base: &str,
        _method: &str,
        path: &str,
        body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        if path != "/v1/writers/append-batch" {
            return Ok(TransportReply {
                status: 200,
                body: json!({"results": [{"data": {"session_epoch": 7}}]}),
            });
        }
        let started = std::time::Instant::now();
        let records = body
            .and_then(|body| body.get("records"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let keys: Vec<Vec<u8>> = records
            .iter()
            .filter_map(|record| {
                record
                    .get("key_base64")
                    .and_then(Value::as_str)
                    .and_then(|value| base64::engine::general_purpose::STANDARD.decode(value).ok())
                    .map(|_| Vec::new())
                    .or_else(|| {
                        record.get("key_base64").and_then(Value::as_str).map(|v| {
                            base64::engine::general_purpose::STANDARD
                                .decode(v)
                                .unwrap_or_default()
                        })
                    })
            })
            .collect();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        self.calls.lock().unwrap().push((started, keys));
        let results: Vec<Value> = records
            .iter()
            .map(|record| {
                json!({
                    "message_id": Uuid::nil().to_string(),
                    "cursor": record["request_id"].as_str().unwrap_or_default(),
                    "deduplicated": false,
                })
            })
            .collect();
        Ok(TransportReply {
            status: 200,
            body: json!({ "results": results }),
        })
    }
}

fn pipeline_client(calls: usize) -> (Client, Arc<PipelineTransport>) {
    let _ = calls;
    let transport = Arc::new(PipelineTransport {
        calls: Mutex::new(Vec::new()),
    });
    let client = Client::new(
        transport.clone(),
        vec!["http://node1".to_owned()],
        "test-key",
    )
    .unwrap();
    (client, transport)
}

#[tokio::test]
async fn pump_pipelines_distinct_keys_but_never_one_key() {
    let (client, transport) = pipeline_client(0);
    let writer = open_writer(&client, "orders.wp").await;
    let pumped = writer.batched(Batching {
        linger: std::time::Duration::from_millis(1),
        max_batch: 1,
        max_in_flight: 4,
    });
    // Six records on three keys, interleaved: K1 lands twice, K2 twice,
    // K3 twice — with max_batch 1 each op is its own batch.
    let mut pending = Vec::new();
    for index in 0..6_u8 {
        let key = format!("key-{}", index % 3).into_bytes();
        pending.push(tokio::spawn({
            let pumped = pumped.clone();
            async move {
                pumped
                    .append(Uuid::new_v4(), &key, b"v", index as i64)
                    .await
            }
        }));
    }
    for task in pending {
        task.await.unwrap().unwrap();
    }
    pumped.flush().await.unwrap();
    let calls = transport.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 6);
    // Pipelined: later batches started before earlier ones finished
    // (each request takes ~20 ms; sequential starts would be ~20 ms
    // apart).
    let first_start = calls[0].0;
    let starts: Vec<std::time::Duration> = calls
        .iter()
        .map(|(instant, _)| instant.duration_since(first_start))
        .collect();
    assert!(
        *starts.last().unwrap() < std::time::Duration::from_millis(120),
        "pipelined rounds should overlap, not serialize: {starts:?}"
    );
    // Per-Key order preserved: calls for key-1 strictly precede in
    // submission order.
    let key1_starts: Vec<_> = calls
        .iter()
        .filter(|(_, keys)| keys.iter().any(|key| key == b"key-1"))
        .map(|(instant, _)| *instant)
        .collect();
    assert!(key1_starts.windows(2).all(|pair| pair[0] < pair[1]));
    // Every batch carried exactly one key (max_batch 1).
    assert!(calls.iter().all(|(_, keys)| keys.len() == 1));
}

#[tokio::test]
async fn pump_completion_wakes_blocked_rounds_promptly() {
    let (client, transport) = pipeline_client(0);
    let writer = open_writer(&client, "orders.wp").await;
    let pumped = writer.batched(Batching {
        linger: std::time::Duration::from_millis(1),
        max_batch: 1,
        max_in_flight: 1,
    });
    // One key twice, then an unrelated key: the second same-Key op must
    // wait for round 1 to finish but round 3 rides immediately after.
    let first = tokio::spawn({
        let pumped = pumped.clone();
        async move { pumped.append(Uuid::new_v4(), b"a", b"1", 1).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let second = tokio::spawn({
        let pumped = pumped.clone();
        async move { pumped.append(Uuid::new_v4(), b"a", b"2", 2).await }
    });
    let third = tokio::spawn({
        let pumped = pumped.clone();
        async move { pumped.append(Uuid::new_v4(), b"b", b"3", 3).await }
    });
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    third.await.unwrap().unwrap();
    pumped.flush().await.unwrap();
    let calls = transport.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 3);
    // Key-blocked round 2 starts promptly after round 1 — a completion
    // notification, not a 500 ms poll, wakes the pump.
    let gap = calls[1].0.duration_since(calls[0].0);
    assert!(
        gap < std::time::Duration::from_millis(100),
        "completion must wake the pump promptly, not at 500 ms: {gap:?}"
    );
}

#[tokio::test]
async fn pump_groups_same_key_ops_into_one_batch_in_order() {
    let (client, transport) = pipeline_client(0);
    let writer = open_writer(&client, "orders.wg").await;
    let pumped = writer.batched(Batching {
        linger: std::time::Duration::from_millis(30),
        max_batch: 512,
        max_in_flight: 4,
    });
    let mut pending = Vec::new();
    for index in 0..20_u8 {
        pending.push(tokio::spawn({
            let pumped = pumped.clone();
            async move {
                pumped
                    .append(Uuid::new_v4(), b"same", b"v", index as i64)
                    .await
            }
        }));
    }
    for task in pending {
        task.await.unwrap().unwrap();
    }
    pumped.flush().await.unwrap();
    let calls = transport.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1, "20 same-Key ops must form one batch");
    assert_eq!(calls[0].1.len(), 20);
}
