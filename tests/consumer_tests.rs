//! Consumer supervision tests: the stub holds a pending page — once
//! queued it keeps answering fetches until an ack lands, exactly like
//! the server redelivering an unacknowledged page. Covers
//! `feed.consumer()` plus the Store find/find_by/get_or_default helpers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

use whitewater::*;

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

fn session() -> Value {
    json!({
        "reader_id": Uuid::nil().to_string(),
        "feed_id": Uuid::nil().to_string(),
        "session_epoch": 3,
        "capacity": 128,
        "delivered_cursor": null,
        "acknowledged_cursor": null,
    })
}

fn wcl_ok() -> Value {
    json!({
        "request_id": Uuid::nil().to_string(),
        "revision": 9,
        "authority": "control_plane",
        "warning": null,
        "results": [{"statement": "s", "message": "ok", "data": {}}],
    })
}

fn record(key: &[u8], payload: &[u8], cursor: &str) -> Value {
    json!({
        "cursor": cursor,
        "message_id": Uuid::nil().to_string(),
        "producer_id": Uuid::nil().to_string(),
        "sequence": 7,
        "event_time_ns": "1697040000123456789",
        "ingest_time_ns": "1697040000223456789",
        "key_base64": B64.encode(key),
        "payload_base64": B64.encode(payload),
        "metadata_base64": {},
    })
}

fn page(delivered: Option<&str>, records: Vec<Value>) -> Value {
    json!({
        "records": records,
        "delivered_cursor": delivered,
        "acknowledged_cursor": null,
        "recommended_capacity": 0,
        "retry_after_ms": 0,
    })
}

/// Stateful transport: queued pages become `pending` on first fetch and
/// keep answering until `/v1/readers/ack` clears them — the redelivery
/// model the supervise loop is tested against.
struct ConsumerStub {
    requests: Mutex<Vec<(String, String, Value)>>,
    pending: Mutex<Option<Value>>,
    queued: Mutex<VecDeque<Value>>,
    open_replies: Mutex<VecDeque<(u16, Value)>>,
    wcl: Mutex<(u16, Value)>,
    store: Mutex<std::collections::BTreeMap<String, Value>>,
    opens: AtomicUsize,
}

impl ConsumerStub {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            pending: Mutex::new(None),
            queued: Mutex::new(VecDeque::new()),
            open_replies: Mutex::new(VecDeque::new()),
            wcl: Mutex::new((200, wcl_ok())),
            store: Mutex::new(std::collections::BTreeMap::new()),
            opens: AtomicUsize::new(0),
        }
    }

    fn calls(&self, path: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p, _)| p == path)
            .count()
    }

    fn bodies(&self, path: &str) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p, _)| p == path)
            .map(|(_, _, b)| b.clone())
            .collect()
    }
}

#[async_trait]
impl Transport for ConsumerStub {
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
        let (status, response) = match path {
            "/v1/admin/wcl" => self.wcl.lock().unwrap().clone(),
            "/v1/readers/open" => {
                self.opens.fetch_add(1, Ordering::SeqCst);
                self.open_replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or((200, session()))
            }
            "/v1/readers/fetch" => {
                let mut pending = self.pending.lock().unwrap();
                let page = match pending.take() {
                    Some(page) => {
                        *pending = Some(page.clone());
                        page
                    }
                    None => match self.queued.lock().unwrap().pop_front() {
                        Some(page) => {
                            *pending = Some(page.clone());
                            page
                        }
                        None => page(None, Vec::new()),
                    },
                };
                (200, page)
            }
            "/v1/readers/ack" => {
                *self.pending.lock().unwrap() = None;
                (200, session())
            }
            "/v1/readers/close" => (200, session()),
            _ => (
                200,
                self.store
                    .lock()
                    .unwrap()
                    .get(path)
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            ),
        };
        Ok(TransportReply {
            status,
            body: response,
        })
    }
}

fn client() -> (Client, Arc<ConsumerStub>) {
    let stub = Arc::new(ConsumerStub::new());
    let client = Client::new(
        stub.clone(),
        vec!["http://node1".to_owned(), "http://node2".to_owned()],
        "test-key",
    )
    .expect("test client");
    (client, stub)
}

#[tokio::test]
async fn next_delivers_each_record_and_acks_the_page() {
    let (client, stub) = client();
    stub.queued.lock().unwrap().push_back(page(
        Some("c2"),
        vec![record(b"k1", b"alpha", "c1"), record(b"k2", b"beta", "c2")],
    ));
    let feed = client.feed("users.update.request");
    let mut consumer = feed
        .consumer("enricher")
        .backoff(Duration::from_millis(10), Duration::from_millis(50));

    let mut seen = Vec::new();
    while let Some(message) = consumer.next().await.expect("next") {
        seen.push(message.value);
        if seen.len() == 2 {
            break;
        }
    }
    // The page acks after the last record delivered; let the task land it.
    for _ in 0..100 {
        if stub.calls("/v1/readers/ack") == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(seen, vec!["alpha", "beta"]);
    assert_eq!(stub.calls("/v1/readers/ack"), 1);
    let ack_body = &stub.bodies("/v1/readers/ack")[0];
    assert_eq!(ack_body["cursor"], json!("c2"));

    consumer.close().await.expect("close");
    assert!(stub.calls("/v1/readers/close") >= 1);
}

#[tokio::test]
async fn run_handler_failure_skips_ack_and_redelivers() {
    let (client, stub) = client();
    stub.queued
        .lock()
        .unwrap()
        .push_back(page(Some("c1"), vec![record(b"k1", b"alpha", "c1")]));
    let feed = client.feed("users.update.request");
    let attempts = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(Mutex::new(Vec::<SdkError>::new()));
    let errors_sink = errors.clone();
    let mut consumer = feed
        .consumer("enricher")
        .backoff(Duration::from_millis(10), Duration::from_millis(50))
        .on_error(move |error| errors_sink.lock().unwrap().push(error.clone()));
    let stop = consumer.handle();
    let handler_attempts = attempts.clone();
    consumer
        .run(move |message| {
            let stop = stop.clone();
            let attempts = handler_attempts.clone();
            async move {
                assert_eq!("alpha", message.value);
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(SdkError::Codec("transient handler bug".to_owned()));
                }
                stop.stop(); // safe inside the handler
                Ok(())
            }
        })
        .await
        .expect("run");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(stub.calls("/v1/readers/ack"), 1); // only the replay acks
    assert!(stub.opens.load(Ordering::SeqCst) >= 2);
    assert!(errors
        .lock()
        .unwrap()
        .iter()
        .any(|e| matches!(e, SdkError::Codec(_))));
}

#[tokio::test]
async fn stale_session_reopens_and_resumes() {
    let (client, stub) = client();
    stub.open_replies
        .lock()
        .unwrap()
        .push_back((409, json!({"error": "fenced", "code": "stale_epoch"})));
    stub.queued
        .lock()
        .unwrap()
        .push_back(page(Some("c1"), vec![record(b"k1", b"alpha", "c1")]));
    let feed = client.feed("users.update.request");
    let mut consumer = feed
        .consumer("enricher")
        .backoff(Duration::from_millis(10), Duration::from_millis(50));
    let stop = consumer.handle();

    consumer
        .run(move |message| {
            let stop = stop.clone();
            async move {
                assert_eq!("alpha", message.value);
                stop.stop();
                Ok(())
            }
        })
        .await
        .expect("run");

    assert!(stub.opens.load(Ordering::SeqCst) >= 2);
    assert_eq!(stub.calls("/v1/readers/ack"), 1);
}

#[tokio::test]
async fn startup_refusal_fails_fast() {
    let (client, stub) = client();
    *stub.wcl.lock().unwrap() = (400, json!({"error": "denied", "code": "refused"}));
    let feed = client.feed("users.update.request");
    let mut consumer = feed.consumer("enricher");

    let error = consumer
        .next()
        .await
        .expect_err("startup refusal must surface");
    assert!(matches!(error, SdkError::Refused(_)));
    assert_eq!(stub.opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_reader_name_fails_fast_not_loops() {
    let (client, stub) = client();
    *stub.wcl.lock().unwrap() = (400, json!({"error": "bad name", "code": "refused"}));
    let feed = client.feed("users.update.request");
    let mut consumer = feed.consumer("Bad Name!");

    let result = consumer.run(|_| async { Ok(()) }).await;
    assert!(matches!(result, Err(SdkError::Refused(_))));
    assert_eq!(stub.opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn store_find_find_by_and_get_or_default() {
    let (client, stub) = client();
    let hit = |value: &str| {
        json!({
            "found": true,
            "key_base64": B64.encode(b"a@x.io"),
            "payload_base64": B64.encode(value.as_bytes()),
            "applied_seq": 7,
            "committed_seq": 7,
            "behind": false,
            "served_by": "storage-1",
        })
    };
    let rows = |value: &str| {
        json!({
            "rows": [{
                "key_base64": B64.encode(b"a@x.io"),
                "payload_base64": B64.encode(value.as_bytes()),
            }],
            "applied_seq": 7,
            "committed_seq": 7,
            "behind": false,
            "served_by": "storage-1",
        })
    };
    stub.store
        .lock()
        .unwrap()
        .insert("/v1/state-stores/lookup".to_owned(), hit("alice"));
    stub.store
        .lock()
        .unwrap()
        .insert("/v1/state-stores/get-by".to_owned(), rows("alice"));

    let profiles = client.store_with("users.profiles", Codec::utf8(), Codec::utf8());
    assert_eq!(
        profiles.find(&"a@x.io".to_owned()).await.expect("find"),
        Some("alice".to_owned())
    );
    assert_eq!(
        profiles
            .find_by("user_id", TypedValue::Text("u-1".to_owned()))
            .await
            .expect("find_by"),
        Some("alice".to_owned())
    );

    // Misses fall through to None / the caller default.
    *stub
        .store
        .lock()
        .unwrap()
        .get_mut("/v1/state-stores/lookup")
        .unwrap() = json!({"found": false, "applied_seq": 7, "committed_seq": 7});
    *stub
        .store
        .lock()
        .unwrap()
        .get_mut("/v1/state-stores/get-by")
        .unwrap() = json!({"rows": [], "applied_seq": 7, "committed_seq": 7});
    assert_eq!(
        profiles.find(&"ghost".to_owned()).await.expect("miss"),
        None
    );
    assert_eq!(
        profiles
            .get_or_default(&"ghost".to_owned(), "fallback".to_owned())
            .await
            .expect("get_or_default"),
        "fallback"
    );
}
