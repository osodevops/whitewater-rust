//! Sustained-throughput benchmark over the public SDK surface — the
//! same `BENCH_RESULT {json}` contract as `scripts/bench-feed.py` and
//! `scripts/bench-kafka-sdk.py` so SDK-vs-SDK comparisons are
//! apples-to-apples (Kafka's Java client vs this client, bytes both
//! ways).
//!
//! ```text
//! BENCH_RECORDS=60000 BENCH_WRITERS=8 BENCH_BATCH=512 \
//!   cargo run --release --example bench_feed
//! ```
//!
//! Environment: `WHITEWATER_ENDPOINTS` (csv, default
//! `http://localhost:7771..7773`), `WHITEWATER_ADMIN_API_KEY`,
//! `BENCH_RECORDS`, `BENCH_BYTES`, `BENCH_WRITERS`, `BENCH_BATCH`,
//! `BENCH_FEEDS` (parallel pipelines = Kafka partition count),
//! `BENCH_PAGE_LIMIT`, `BENCH_READ_FORMAT` (`binary`|`json`),
//! `BENCH_MODE` (`all`|`write`|`read`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use serde_json::json;
use uuid::Uuid;
use whitewater::{Client, TemporaryFetch};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_str(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64) * quantile).ceil() as usize;
    sorted[index.min(sorted.len()) - 1]
}

#[derive(Default)]
struct WriteStats {
    latencies_ms: Mutex<Vec<f64>>,
    acknowledged: AtomicU64,
    errors: AtomicU64,
}

async fn write_worker(
    client: Client,
    writer_name: String,
    share: u64,
    batch: u64,
    payload: Vec<u8>,
    base_index: u64,
    stats: std::sync::Arc<WriteStats>,
) {
    let writer = match client.writer(&writer_name).await {
        Ok(writer) => writer,
        Err(error) => {
            stats.errors.fetch_add(share, Ordering::Relaxed);
            eprintln!("writer {writer_name} session failed: {error}");
            return;
        }
    };
    let mut sent = 0u64;
    let mut index = base_index;
    while sent < share {
        let take = batch.min(share - sent);
        // Shared request_ids across attempts: a retry after an
        // ambiguous outcome deduplicates server-side.
        let records: Vec<(Uuid, Vec<u8>, Vec<u8>, i64)> = (0..take)
            .map(|offset| {
                (
                    Uuid::new_v4(),
                    format!("key-{:012}", index + offset).into_bytes(),
                    payload.clone(),
                    1_700_000_000_000_000_000i64 + (index + offset) as i64,
                )
            })
            .collect();
        index += take;
        sent += take;
        let mut attempts = 0;
        loop {
            let started = Instant::now();
            match writer.append_batch(&records).await {
                Ok(_) => {
                    stats
                        .latencies_ms
                        .lock()
                        .expect("latencies")
                        .push(started.elapsed().as_secs_f64() * 1_000.0);
                    stats.acknowledged.fetch_add(take, Ordering::Relaxed);
                    break;
                }
                Err(error) => {
                    attempts += 1;
                    if attempts >= 8 {
                        eprintln!("{writer_name} append refused after retries: {error}");
                        stats.errors.fetch_add(take, Ordering::Relaxed);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}

async fn read_worker(
    client: Client,
    feed: String,
    expected: u64,
    page_limit: u64,
    binary: bool,
) -> Result<(u64, u64, f64), String> {
    let mut after: Option<String> = None;
    let mut records = 0u64;
    let mut bytes = 0u64;
    let mut stalled = 0u32;
    let started = Instant::now();
    while records < expected {
        let options = TemporaryFetch {
            after: after.clone(),
            limit: page_limit,
            new_only: false,
            tail: false,
            wait_ms: Some(5_000),
            after_event_time_ns: None,
        };
        let page = if binary {
            client.temporary_fetch_frames(&feed, options).await
        } else {
            client.temporary_fetch(&feed, options).await
        }
        .map_err(|error| format!("{feed} fetch failed: {error}"))?;
        if page.records.is_empty() {
            stalled += 1;
            if stalled > 24 {
                return Err(format!(
                    "{feed}: no progress for ~2min with {records}/{expected} records"
                ));
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        stalled = 0;
        records += page.records.len() as u64;
        bytes += page
            .records
            .iter()
            .map(|record| record.payload.len() as u64)
            .sum::<u64>();
        match page.next_cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    Ok((records, bytes, started.elapsed().as_secs_f64()))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoints: Vec<String> = std::env::var("WHITEWATER_ENDPOINTS")
        .map(|csv| {
            csv.split(',')
                .map(|endpoint| endpoint.trim().to_owned())
                .collect()
        })
        .unwrap_or_else(|_| {
            let base = env_u64("BENCH_BASE_PORT", 7771);
            (0..3)
                .map(|offset| format!("http://localhost:{}", base + offset))
                .collect()
        });
    let api_key = env_str(
        "WHITEWATER_ADMIN_API_KEY",
        "whitewater-local-development-admin-key",
    );
    let records_total = env_u64("BENCH_RECORDS", 20_000);
    let payload_bytes = env_u64("BENCH_BYTES", 1024) as usize;
    let writers = env_u64("BENCH_WRITERS", 4) as usize;
    let batch = env_u64("BENCH_BATCH", 50);
    let feeds = env_u64("BENCH_FEEDS", 1) as usize;
    let page_limit = env_u64("BENCH_PAGE_LIMIT", 65_536);
    let binary_read = env_str("BENCH_READ_FORMAT", "binary") == "binary";
    let mode = env_str("BENCH_MODE", "all");

    let client = Client::http(endpoints, api_key)?;
    let suffix = format!(
        "{}{:04x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            % 1_000_000,
        Uuid::new_v4().as_u128() & 0xFFFF
    );
    // BENCH_DOMAIN pins the domain name so a `read`-only run targets
    // data written by an earlier pass; pair it with BENCH_PROVISIONED=1
    // to skip creation against the existing objects.
    let domain = env_str("BENCH_DOMAIN", "").trim().to_owned();
    let domain = if domain.is_empty() {
        format!("benchsdk{suffix}")
    } else {
        domain
    };
    let provisioned = env_str("BENCH_PROVISIONED", "0") == "1";
    let feed_names: Vec<String> = (0..feeds)
        .map(|index| {
            if feeds == 1 {
                format!("{domain}.events")
            } else {
                format!("{domain}.events{index}")
            }
        })
        .collect();

    let writer_names: Vec<String> = (0..writers)
        .map(|index| format!("benchsdk{suffix}w{index}"))
        .collect();
    // Provision domain, feeds, and one declared Writer per worker —
    // skipped when BENCH_PROVISIONED=1 marks existing objects.
    if !provisioned {
        let mut script = format!("CREATE DOMAIN {domain}; ");
        for feed in &feed_names {
            script.push_str(&format!("CREATE FEED {feed}; "));
        }
        for (index, name) in writer_names.iter().enumerate() {
            script.push_str(&format!(
                "CREATE WRITER {name} TO {}; ",
                feed_names[index % feeds]
            ));
        }
        client.wcl(&script).await?;
    }

    // writer -> feed assignment determines per-feed expected read counts.
    let shares: Vec<u64> = (0..writers)
        .map(|index| {
            records_total / writers as u64 + u64::from(index < (records_total as usize % writers))
        })
        .collect();
    let expected_per_feed: Vec<u64> = (0..feeds)
        .map(|feed_index| {
            (0..writers)
                .filter(|writer| writer % feeds == feed_index)
                .map(|writer| shares[writer])
                .sum()
        })
        .collect();

    if mode != "read" {
        let stats = std::sync::Arc::new(WriteStats::default());
        let payload = vec![0xAB; payload_bytes];
        let started = Instant::now();
        let mut handles = Vec::with_capacity(writers);
        for index in 0..writers {
            handles.push(tokio::spawn(write_worker(
                client.clone(),
                writer_names[index].clone(),
                shares[index],
                batch,
                payload.clone(),
                // disjoint key namespace per worker
                records_total * 1_000 + index as u64 * records_total,
                stats.clone(),
            )));
        }
        for handle in handles {
            handle.await?;
        }
        let elapsed = started.elapsed().as_secs_f64();
        let acknowledged = stats.acknowledged.load(Ordering::Relaxed);
        let errors = stats.errors.load(Ordering::Relaxed);
        let mut latencies = stats.latencies_ms.lock().expect("latencies").clone();
        latencies.sort_by(|a, b| a.partial_cmp(b).expect("latency"));
        let result = json!({
            "system": "whitewater-rust-sdk",
            "mode": "write",
            "records": acknowledged,
            "record_bytes": payload_bytes,
            "writers": writers,
            "batch": batch,
            "feeds": feeds,
            "seconds": elapsed,
            "records_per_sec": acknowledged as f64 / elapsed,
            "mib_per_sec": (acknowledged * payload_bytes as u64) as f64 / elapsed / 1_048_576.0,
            "p50_ms": percentile(&latencies, 0.50),
            "p95_ms": percentile(&latencies, 0.95),
            "p99_ms": percentile(&latencies, 0.99),
            "errors": errors,
        });
        println!("BENCH_RESULT {result}");
    }

    if mode != "write" {
        let workers: Vec<_> = feed_names
            .iter()
            .enumerate()
            .map(|(index, feed)| {
                tokio::spawn(read_worker(
                    client.clone(),
                    feed.clone(),
                    expected_per_feed[index],
                    page_limit,
                    binary_read,
                ))
            })
            .collect();
        let mut records = 0u64;
        let mut bytes = 0u64;
        let mut max_elapsed = 0.0f64;
        for worker in workers {
            let (count, size, elapsed) = worker.await?.map_err(std::io::Error::other)?;
            records += count;
            bytes += size;
            max_elapsed = max_elapsed.max(elapsed);
        }
        let elapsed = max_elapsed.max(0.001);
        let result = json!({
            "system": "whitewater-rust-sdk",
            "mode": "read",
            "format": if binary_read { "binary" } else { "json" },
            "records": records,
            "feeds": feeds,
            "readers": feeds,
            "page_limit": page_limit,
            "seconds": elapsed,
            "records_per_sec": records as f64 / elapsed,
            "mib_per_sec": bytes as f64 / elapsed / 1_048_576.0,
        });
        println!("BENCH_RESULT {result}");
    }
    Ok(())
}
