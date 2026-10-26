//! Domain-first example against a local development Node — the
//! strongly-typed façade: fetch a Domain, list its feeds, and open
//! Reader/Writer sessions through the fluent builders. Run with:
//!
//!   WHITEWATER_ENDPOINTS=http://localhost:7371 cargo run --example domain_model
//!
//! (defaults to the standard three-Node development endpoints and the
//! documented development admin key).

use std::env;

use whitewater::{Client, ReaderStart};

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

    let domain_name = format!(
        "shop{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    client.wcl(&format!("CREATE DOMAIN {domain_name};")).await?;

    // get_domain verifies existence and materializes the catalog
    // snapshot in one batched control read; create_feed declares the
    // feed inside the Domain's prefix.
    let shop = client.get_domain(&domain_name).await?;
    shop.create_feed("orders").await?;
    println!("domain id: {}", shop.domain_id().await?);
    println!("feeds:     {:?}", shop.feed_names().await?);

    // Pure name composition — no I/O.
    let orders = shop.feed("orders")?;

    // Builders hang off the Domain — a typed Feed handle keeps its
    // codecs (client.create_writer(&shop, &orders)), while the
    // relative-name form shop.create_writer("orders") resolves inside
    // the Domain.
    let writer = client
        .create_writer(&shop, &orders)?
        .writer_id("ingest")
        .open()
        .await?;
    writer
        .append(&"order-1".to_owned(), &b"created".to_vec())
        .await?;

    // Durable consume identity + first-time start position; reopening
    // the same id resumes the acknowledged frontier.
    let mut reader = shop
        .create_reader("orders")?
        .reader_id("audit")
        .reader_start_point(ReaderStart::Beginning)
        .capacity(64)
        .open()
        .await?;
    let page = reader.fetch(16).await?;
    for message in &page.records {
        println!("{:?} -> {:?}", message.key, message.value);
    }
    reader.ack().await?;
    reader.close().await?;
    Ok(())
}
