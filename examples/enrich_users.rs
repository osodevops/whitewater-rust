//! The permanent enrichment consumer: read users.update.request, look
//! up the profile in a StateStore (primary key, then the declared
//! `user_id` named key), emit users.update.enriched.request — or the
//! DLQ when no profile matches — and bump day/week/month counters.
//!
//! The whole consumer loop is:
//!
//!   while let Some(message) = consumer.next().await? {
//!       process(&message).await?;
//!   }
//!
//! Open/fetch/ack/reopen/backoff/session-close stay inside the SDK —
//! the named Reader's durable cursor resumes across restarts; a page
//! acks only after every record delivered cleanly, so an error
//! redelivers. Run with:
//!
//!   cargo run --example enrich_users

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use whitewater::{Client, Codec, FeedEvent, SdkError, TypedValue};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UpdateRequest {
    request_id: String,
    user_email: String,
    user_id: String,
    changes: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserProfile {
    user_id: String,
    email: String,
    display_name: String,
    region: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EnrichedUpdate {
    request_id: String,
    user_id: String,
    email: String,
    display_name: String,
    region: String,
    changes: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DeadLetter {
    reason: String,
    source_feed: String,
    source_message_id: String,
    source_cursor: String,
    original: UpdateRequest,
}

#[tokio::main]
async fn main() -> Result<(), SdkError> {
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
        "enrich{:x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    client
        .wcl(&format!(
            "CREATE DOMAIN {domain};\
             CREATE FEED {domain}.users.update.request;\
             CREATE FEED {domain}.users.update.enriched.request;\
             CREATE FEED {domain}.users.update.dlq;\
             CREATE STATE STORE {domain}.users.profiles SOURCE MANUAL\
                 NAMED KEYS (user_id UNIQUE);\
             CREATE STATE STORE {domain}.users.stats SOURCE MANUAL;"
        ))
        .await?;

    let input = client.feed_with(
        &format!("{domain}.users.update.request"),
        Codec::utf8(),
        Codec::json(),
    );
    let output = client.feed_with(
        &format!("{domain}.users.update.enriched.request"),
        Codec::utf8(),
        Codec::json(),
    );
    let dlq = client.feed_with(
        &format!("{domain}.users.update.dlq"),
        Codec::utf8(),
        Codec::json(),
    );

    let profiles = client.store_with(
        &format!("{domain}.users.profiles"),
        Codec::utf8(),
        Codec::json(),
    );
    let counters = client.store_with(
        &format!("{domain}.users.stats"),
        Codec::utf8(),
        Codec::json(),
    );

    let out_writer = output.writer("enricher").await?;
    let dlq_writer = dlq.writer("enricher").await?;

    let mut consumer = input.consumer("enricher");
    while let Some(message) = consumer.next().await? {
        let request: &UpdateRequest = &message.value;

        // Value-level lookups: find by primary key (email), fall back
        // to the declared `user_id` named key — the second index.
        let profile: Option<UserProfile> = match profiles.find(&request.user_email).await? {
            Some(profile) => Some(profile),
            None => {
                profiles
                    .find_by("user_id", TypedValue::Text(request.user_id.clone()))
                    .await?
            }
        };

        // Replay-safe effects: the record's message_id pins the
        // append — a crash between append and ack redelivers the input
        // but dedupes the emit to the same identity.
        let effect_id = message.message_id.unwrap_or_else(uuid::Uuid::new_v4);

        let Some(profile) = profile else {
            dlq_writer
                .append_event(
                    FeedEvent::new(
                        request.user_email.clone(),
                        DeadLetter {
                            reason: "user not found on users.profiles".to_owned(),
                            source_feed: input.name().to_owned(),
                            source_message_id: message
                                .message_id
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            source_cursor: message.cursor.clone().unwrap_or_default(),
                            original: request.clone(),
                        },
                    )
                    .with_request_id(effect_id),
                )
                .await?;
            continue;
        };

        out_writer
            .append_event(
                FeedEvent::new(
                    profile.user_id.clone(),
                    EnrichedUpdate {
                        request_id: request.request_id.clone(),
                        user_id: profile.user_id,
                        email: profile.email,
                        display_name: profile.display_name,
                        region: profile.region,
                        changes: request.changes.clone(),
                    },
                )
                .with_request_id(effect_id),
            )
            .await?;

        // get_or_default: a miss counts as zero — counters stay one line.
        let (day, week, month) = buckets(message.event_time_ns);
        for bucket in [day, week, month] {
            let current: i64 = counters.get_or_default(&bucket, 0).await?;
            counters
                .put(uuid::Uuid::new_v4(), &bucket, &(current + 1))
                .await?;
        }
    }
    consumer.close().await?;
    Ok(())
}

fn buckets(event_time_ns: i64) -> (String, String, String) {
    let secs = event_time_ns / 1_000_000_000;
    let days = secs.div_euclid(86_400);
    // Day/month from civil arithmetic — enough for a demo; production
    // uses a calendar crate.
    let (year, month, day) = civil_from_days(days);
    let week = days.div_euclid(7);
    (
        format!("updates.day.{year:04}-{month:02}-{day:02}"),
        format!("updates.week.{year:04}-W{week:02}"),
        format!("updates.month.{year:04}-{month:02}"),
    )
}

/// Howard Hinnant's civil-from-days algorithm.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096).div_euclid(365);
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2).div_euclid(153);
    let day = doy - (153 * mp + 2).div_euclid(5) + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    (year + (month <= 2) as i64, month, day)
}
