//! Cross-domain pipes: a `purchasing` domain owns purchase-order
//! intake; a `warehouse` domain owns fulfilment. The relay Pipe is
//! declared and owned by `warehouse`, but its Subscription sources the
//! `purchasing` Feed and its output lands on the `warehouse` Feed —
//! endpoints cross Domains through `Feed` handles or `Scoped::abs`,
//! while the Pipe's name and input Subscription stay in-domain.
//!
//!   WHITEWATER_ENDPOINTS=http://localhost:7371 cargo run --example cross_domain
//!
//! (defaults to the standard three-Node development endpoints and the
//! documented development admin key).

use std::env;

use whitewater::{Client, ReaderStart, Scoped};

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

    let suffix = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );

    // Two business domains — purchasing owns order intake, warehouse
    // owns inbound fulfilment.
    let purchasing = client.create_domain(&format!("purchasing{suffix}")).await?;
    let warehouse = client.create_domain(&format!("warehouse{suffix}")).await?;

    purchasing.create_feed("orders").await?;
    warehouse.create_feed("inbound").await?;

    // The warehouse Subscription pulls from the purchasing Feed — the
    // source crosses Domains via Scoped::abs (or a Feed handle:
    // `purchasing.feed("orders")?` carries the absolute name).
    warehouse
        .create_subscription(
            "po.intake",
            Scoped::abs(format!("{}.orders", purchasing.name())),
            Some(ReaderStart::Beginning),
        )
        .await?;

    // The relay Pipe is owned by warehouse; its output Feed is
    // in-domain here, but it could just as well be Scoped::abs or a
    // Feed handle on another Domain.
    warehouse
        .define_forward_pipe("po.relay", "po.intake", "inbound")
        .await?;

    println!(
        "declared {}.po.relay: {}.orders -> {}.inbound",
        warehouse.name(),
        purchasing.name(),
        warehouse.name()
    );
    for info in warehouse.pipe_infos().await? {
        println!(
            "pipe {} stage={} status={}",
            info.name, info.stage, info.status
        );
    }
    Ok(())
}
