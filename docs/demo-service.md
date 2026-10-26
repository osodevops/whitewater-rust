# Demo: order-fulfillment service

A complete microservice shape: provision a Domain + Feed, append
orders, and pump them through a processor with at-least-once
semantics, a dead-letter path, and graceful shutdown.

The contract in one line: **`Ok(true)` = processed, `Err` = replay** —
the SDK acks a page only after every record in it has been handled.

```rust
use whitewater::{Client, ReaderStart, SdkError};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    let client = Client::http(
        vec!["http://localhost:7371".to_owned(), "http://localhost:7372".to_owned()],
        std::env::var("WHITEWATER_API_KEY").unwrap_or_default(),
    )?;

    // ---- provision ---------------------------------------------------
    client.wcl("CREATE DOMAIN shop;").await?;
    let shop = client.get_domain("shop").await?;
    shop.create_feed("orders").await?;
    shop.create_feed("orders.dlq").await?;        // dead-letter feed

    // ---- produce (usually a different service) -----------------------
    let writer = shop
        .create_writer("orders")?
        .writer_id("orders.ingest")
        .open()
        .await?;
    writer.append(&"order-1".to_owned(), &b"{\"total\":42}".to_vec()).await?;

    // ---- consume -----------------------------------------------------
    let dead_letter = shop
        .create_writer("orders.dlq")?
        .writer_id("fulfillment.dlq")
        .open()
        .await?;
    let mut reader = shop
        .create_reader("orders")?
        .reader_id("fulfillment")                  // durable id — survives restarts
        .reader_start_point(ReaderStart::Beginning) // first-declare only
        .max_page_size(250)                        // max records per fetch page
        .open()
        .await?;

    // for_each is a future — tokio::select! is the off-switch, no
    // dedicated thread anywhere.
    let pump = reader.for_each(|m| {
        let dlq = dead_letter.clone();              // FeedWriter is Clone
        async move {
            match upsert(order_id(&m), order(&m)).await {
                Ok(()) => Ok(true),                                  // handled → page acks
                Err(e) if permanent(&e) => {
                    let _ = dlq.append(&m.key.clone().unwrap_or_default(), &m.value).await;
                    Ok(true)                                         // quarantined → continue → acked
                }
                Err(e) => Err(e),                                    // transient → stop → replay
            }
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}        // graceful stop
        result = pump => {
            // A dead pump surfaces the handler/transport error here —
            // restart or alert.
            if let Err(e) = result {
                eprintln!("reader pump died: {e}");
            }
        }
    }

    // close() releases the session fence; the in-flight page stays
    // unacked and replays on the next open.
    reader.close().await
}
```

## Notes

- `reader_id` is durable application state — the Kafka `groupId`
  analogue. `reader_start_point` applies only on first declare;
  restarts resume the acknowledged frontier.
- Acks are per fetched page. A crash mid-page replays the whole page —
  that's what makes it at-least-once. Process idempotently.
- `max_page_size(250)` bounds both the page and the replay window.
- The pump adopts the server's `recommended_capacity` as it runs.
  Manual mode: `fetch()` → process → `ack()`. Never ack before
  processing — that's at-most-once.
