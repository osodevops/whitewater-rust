//! Binary-fetch wire tests: scripted transports answer the three
//! frame-page surfaces (`/v1/readers/fetch`, `/v1/subscriptions/members/
//! fetch`, `/v1/feeds/records`) with decoded pages plus the metadata the
//! binary wire carries, while JSON-only transports must fall back to
//! the ordinary JSON calls.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use uuid::Uuid;

use whitewater::*;

/// JSON reply FIFO for session calls plus decoded-page FIFOs for the
/// three binary surfaces — the transport boundary decodes wire bytes
/// before the SDK sees them, so staged pages carry `Record`s.
struct BinaryTransport {
    replies: Mutex<std::collections::VecDeque<(u16, Value)>>,
    pages: Mutex<std::collections::VecDeque<TransportFramePageReply>>,
    meta_pages: Mutex<std::collections::VecDeque<TransportMetaFramePageReply>>,
}

impl BinaryTransport {
    fn new(replies: Vec<(u16, Value)>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            pages: Mutex::new(std::collections::VecDeque::new()),
            meta_pages: Mutex::new(std::collections::VecDeque::new()),
        }
    }

    fn push_page(
        &self,
        records: Vec<whitewater::Record>,
        next_cursor: Option<String>,
        headers: Vec<(String, String)>,
    ) {
        self.pages
            .lock()
            .unwrap()
            .push_back(TransportFramePageReply {
                status: 200,
                page: Some((records, next_cursor)),
                body: Vec::new(),
                headers,
            });
    }
}

#[async_trait]
impl Transport for BinaryTransport {
    async fn request(
        &self,
        _base: &str,
        _method: &str,
        _path: &str,
        _body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        let (status, body) = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or((500, json!({"error": "no scripted reply"})));
        Ok(TransportReply { status, body })
    }

    async fn request_frame_page(
        &self,
        _base: &str,
        _path: &str,
        _body: Option<&Value>,
        _api_key: &str,
        _on_header: Option<tokio::sync::oneshot::Sender<Option<String>>>,
    ) -> Result<TransportFramePageReply, SdkError> {
        self.pages
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| SdkError::Transport("no scripted frame page".to_owned()))
    }

    async fn request_frame_page_get(
        &self,
        _base: &str,
        _path: &str,
        _api_key: &str,
    ) -> Result<TransportFramePageReply, SdkError> {
        self.pages
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| SdkError::Transport("no scripted frame page".to_owned()))
    }

    async fn request_meta_frame_page(
        &self,
        _base: &str,
        _path: &str,
        _body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportMetaFramePageReply, SdkError> {
        self.meta_pages
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| SdkError::Transport("no scripted meta page".to_owned()))
    }
}

fn binary_record(key: &[u8], cursor: &str) -> whitewater::Record {
    whitewater::Record {
        message_id: Some(Uuid::nil()),
        key: key.to_vec(),
        payload: b"v".to_vec(),
        metadata: BTreeMap::new(),
        event_time_ns: 11,
        ingest_time_ns: 22,
        cursor: Some(cursor.to_owned()),
    }
}

fn binary_client(transport: Arc<BinaryTransport>) -> Client {
    Client::new(transport, vec!["http://node1".to_owned()], "k").expect("test client")
}

#[tokio::test]
async fn reader_fetch_binary_page_carries_pacing_headers() {
    let transport = Arc::new(BinaryTransport::new(vec![(
        200,
        json!({"session_epoch": 3, "delivered_cursor": null, "acknowledged_cursor": null}),
    )]));
    let client = binary_client(transport.clone());
    transport.push_page(
        vec![binary_record(b"k1", "cur-1")],
        Some("delivered-9".to_owned()),
        vec![
            (
                "x-whitewater-acknowledged-cursor".to_owned(),
                "acked-8".to_owned(),
            ),
            (
                "x-whitewater-recommended-capacity".to_owned(),
                "128".to_owned(),
            ),
            ("x-whitewater-retry-after-ms".to_owned(), "40".to_owned()),
        ],
    );
    let mut reader = client.reader("orders.rd", 64).await.unwrap();
    let page = reader.fetch(16).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].key, b"k1");
    assert_eq!(page.records[0].cursor.as_deref(), Some("cur-1"));
    assert_eq!(page.delivered_cursor.as_deref(), Some("delivered-9"));
    assert_eq!(page.acknowledged_cursor.as_deref(), Some("acked-8"));
    assert_eq!(page.recommended_capacity, Some(128));
    assert_eq!(page.retry_after_ms, Some(40));
}

#[tokio::test]
async fn reader_fetch_binary_error_maps_the_json_body() {
    let transport = Arc::new(BinaryTransport::new(vec![(
        200,
        json!({"session_epoch": 3}),
    )]));
    let client = binary_client(transport.clone());
    transport
        .pages
        .lock()
        .unwrap()
        .push_back(TransportFramePageReply {
            status: 409,
            page: None,
            body: serde_json::to_vec(&json!({"error": "old", "code": "stale_epoch"})).unwrap(),
            headers: Vec::new(),
        });
    let mut reader = client.reader("orders.rd", 64).await.unwrap();
    assert_eq!(reader.fetch(1).await.unwrap_err(), SdkError::StaleEpoch);
}

#[tokio::test]
async fn member_fetch_binary_page_carries_meta_prelude() {
    let transport = Arc::new(BinaryTransport::new(vec![]));
    let client = binary_client(transport.clone());
    let member = Member::for_testing(client, "orders.flow", Uuid::new_v4(), 2);
    let work = ClaimedWork {
        work_id: Uuid::new_v4(),
        lease_epoch: 5,
        expires_at_tick: Some(90),
    };
    transport
        .meta_pages
        .lock()
        .unwrap()
        .push_back(TransportMetaFramePageReply {
            status: 200,
            meta: Some(json!({
                "positions": {"7": "pos-9"},
                "delivery_attempt": 5,
                "next_delivery_ns": "1700000000000000333"
            })),
            page: Some((
                vec![binary_record(b"mk", "mc-1")],
                Some("frontier-7".to_owned()),
            )),
            body: Vec::new(),
        });
    let page = member.fetch(&work, 4).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].key, b"mk");
    assert_eq!(page.cursor.as_deref(), Some("frontier-7"));
    assert_eq!(page.positions, json!({"7": "pos-9"}));
    assert_eq!(page.delivery_attempt, 5);
    assert_eq!(page.next_delivery_ns, Some(1_700_000_000_000_000_333));
}

#[tokio::test]
async fn member_fetch_json_answer_on_binary_accept_decodes_meta() {
    // A Node that ignores the member-frames Accept answers JSON — the
    // transport surfaces the whole body as `meta` with no page.
    let transport = Arc::new(BinaryTransport::new(vec![]));
    let client = binary_client(transport.clone());
    let member = Member::for_testing(client, "orders.flow", Uuid::new_v4(), 2);
    let work = ClaimedWork {
        work_id: Uuid::new_v4(),
        lease_epoch: 5,
        expires_at_tick: Some(90),
    };
    transport
        .meta_pages
        .lock()
        .unwrap()
        .push_back(TransportMetaFramePageReply {
            status: 200,
            meta: Some(json!({
                "records": [{
                    "message_id": Uuid::nil().to_string(),
                    "key_base64": "ag==",
                    "payload_base64": "dw==",
                    "metadata_base64": {},
                    "event_time_ns": "1",
                    "ingest_time_ns": "2",
                    "cursor": "rc"
                }],
                "cursor": "jc-1",
                "positions": {"3": "p-1"},
                "delivery_attempt": 2
            })),
            page: None,
            body: Vec::new(),
        });
    let page = member.fetch(&work, 4).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].key, b"j");
    assert_eq!(page.cursor.as_deref(), Some("jc-1"));
    assert_eq!(page.delivery_attempt, 2);
}

#[tokio::test]
async fn records_walk_uses_the_binary_get_page() {
    let transport = Arc::new(BinaryTransport::new(vec![]));
    let client = binary_client(transport.clone());
    transport.push_page(
        vec![binary_record(b"ka", "rc-1"), binary_record(b"kb", "rc-2")],
        Some("rc-2".to_owned()),
        Vec::new(),
    );
    let records = client.records("orders.events", None, 2).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].key, b"ka");
    assert_eq!(records[1].cursor.as_deref(), Some("rc-2"));
}

/// A JSON-only scripted transport — every binary method refuses with
/// `Refused`, the same way the trait defaults do.
struct JsonOnlyTransport {
    replies: Mutex<std::collections::VecDeque<(u16, Value)>>,
}

#[async_trait]
impl Transport for JsonOnlyTransport {
    async fn request(
        &self,
        _base: &str,
        _method: &str,
        _path: &str,
        _body: Option<&Value>,
        _api_key: &str,
    ) -> Result<TransportReply, SdkError> {
        let (status, body) = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or((500, json!({"error": "no scripted reply"})));
        Ok(TransportReply { status, body })
    }
}

#[tokio::test]
async fn json_only_transports_fall_back_on_every_binary_wire() {
    let transport = Arc::new(JsonOnlyTransport {
        replies: Mutex::new(
            vec![
                (
                    200,
                    json!({"session_epoch": 2, "delivered_cursor": null, "acknowledged_cursor": null}),
                ),
                (
                    200,
                    json!({
                        "records": [{
                            "message_id": Uuid::nil().to_string(),
                            "key_base64": "aw==",
                            "payload_base64": "dg==",
                            "metadata_base64": {},
                            "event_time_ns": "1",
                            "ingest_time_ns": "2",
                            "cursor": "rc"
                        }],
                        "delivered_cursor": "d1",
                        "acknowledged_cursor": "a0"
                    }),
                ),
                (
                    200,
                    json!({
                        "records": [{
                            "message_id": Uuid::nil().to_string(),
                            "key_base64": "bQ==",
                            "payload_base64": "bg==",
                            "metadata_base64": {},
                            "event_time_ns": "1",
                            "ingest_time_ns": "2",
                            "cursor": "rc"
                        }],
                        "cursor": "c9",
                        "positions": {},
                        "delivery_attempt": 3
                    }),
                ),
                (
                    200,
                    json!([{
                        "message_id": Uuid::nil().to_string(),
                        "key_base64": "aA==",
                        "payload_base64": "dw==",
                        "metadata_base64": {},
                        "event_time_ns": "1",
                        "ingest_time_ns": "2",
                        "cursor": "rc"
                    }]),
                ),
            ]
            .into(),
        ),
    });
    let client = Client::new(transport, vec!["http://node1".to_owned()], "k").expect("client");
    let mut reader = client.reader("orders.rd", 64).await.unwrap();
    assert_eq!(reader.fetch(1).await.unwrap().records.len(), 1);
    let member = Member::for_testing(client.clone(), "orders.flow", Uuid::new_v4(), 1);
    let work = ClaimedWork {
        work_id: Uuid::new_v4(),
        lease_epoch: 1,
        expires_at_tick: None,
    };
    assert_eq!(member.fetch(&work, 1).await.unwrap().records.len(), 1);
    assert_eq!(
        client
            .records("orders.events", None, 1)
            .await
            .unwrap()
            .len(),
        1
    );
}
