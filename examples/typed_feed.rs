//! End-to-end typed feed example against a local development Node:
//! declare a domain/feed over WCL, write JSON records through bound
//! codecs, read them back decoded. Run with:
//!
//!   WHITEWATER_ENDPOINTS=http://localhost:7371 cargo run --example typed_feed
//!
//! (defaults to the standard three-Node development endpoints and the
//! documented development admin key).

use std::env;

use serde::{Deserialize, Serialize};
use whitewater::{Client, Codec, FeedEvent};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    id: String,
    total: i64,
}

#[tokio::main]
async fn main() -> Result<(), whitewater::SdkError> {
    let bases = env::var("WHITEWATER_ENDPOINTS")
        .unwrap_or_else(|_| {
            "http://localhost:7371,http://localhost:7372,http://localhost:7373".to_owned()
        })
        .split(',')
        .map(|base| base.trim().to_owned())
        .collect::<Vec<_>>();
    let api_key = env::var("WHITEWATER_API_KEY")
        .unwrap_or_else(|_| "whitewater-local-development-admin-key".to_owned());
    let client = Client::http(bases, api_key)?;
    client.check_compatible().await?;

    let domain = format!(
        "example{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    client
        .wcl(&format!(
            "CREATE DOMAIN {domain}; CREATE FEED {domain}.events;"
        ))
        .await?;

    // Bind codecs once — every read and write serializes automatically.
    let feed = client.feed_with(
        &format!("{domain}.events"),
        Codec::utf8(),
        Codec::<Order>::json(),
    );

    let writer = feed.writer("checkout").await?;
    writer
        .append_event(
            FeedEvent::new(
                "order-1".to_owned(),
                Order {
                    id: "o-1".to_owned(),
                    total: 4200,
                },
            )
            .with_event_time_ns(1_700_000_000_000_000_000),
        )
        .await?;
    writer
        .append_event(FeedEvent::keyless(Order {
            id: "heartbeat".to_owned(),
            total: 0,
        }))
        .await?;

    let mut reader = feed.reader("audit", 64).await?;
    let page = reader.fetch(16).await?;
    for message in &page.records {
        println!(
            "{:?} -> order {} total {} (event_time_ns {})",
            message.key, message.value.id, message.value.total, message.event_time_ns
        );
    }
    reader.ack().await?;
    reader.close().await?;
    Ok(())
}
