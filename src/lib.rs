//! Typed application client for the Whitewater wire protocol
//! (`sdk/spec/PROTOCOL.md`, `sdk/spec/endpoints.json`). The public model
//! stays `riverbed -> domain -> feed -> key -> cursor -> subscription`:
//! no Range, owner, replica, or partition concept crosses this
//! boundary, and Cursors are opaque pass-through strings.
//!
//! `Transport` is the seam unit tests drive: [`HttpTransport`] is the
//! production reqwest implementation, while tests inject a stub that
//! records requests and returns scripted replies. Every mutating call
//! carries a caller `request_id`; after an ambiguous outcome the SDK
//! retries the identical request rather than regenerating identity.
//!
//! Nanosecond fields are `i64` in Rust and decimal strings on the wire —
//! never floats — matching the spec's scalar contract.
//!
//! # Typed feeds and stores
//!
//! [`Feed<K, V>`] and [`Store<K, V>`] bind [`Codec`]s once so every
//! append, fetch, get, and put serializes automatically. The byte-level
//! [`Client`]/`[`Writer`]`/`[`Reader`]`/`[`StateStore`] remain the
//! explicit escape hatches.
//!
//! ```no_run
//! # async fn demo() -> Result<(), whitewater::SdkError> {
//! use whitewater::{Client, Codec};
//!
//! let client = Client::http(
//!     vec!["https://node1:7371".to_owned()],
//!     "api-key",
//! )?;
//! let orders = client.feed_with(
//!     "orders.events",
//!     Codec::utf8(),
//!     Codec::<serde_json::Value>::json(),
//! );
//! let mut reader = orders.reader("audit", 64).await?;
//! let page = reader.fetch(16).await?;
//! reader.ack().await?;
//! # Ok(())
//! # }
//! ```

mod client;
mod codec;
mod consumer;
mod demand;
mod domain;
mod error;
mod feed;
mod member;
mod model;
mod pipe;
mod reader;
mod store;
mod transport;
mod typed;
mod typed_store;
mod writer;

pub use client::Client;
pub use codec::Codec;
pub use consumer::{ConsumerHandle, FeedConsumer, ReaderStart};
pub use demand::{DemandReport, DemandRow, HotKey};
pub use domain::{Domain, FeedSchema, IntoDomainFeed, ReaderBuilder, SchemaDecl, WriterBuilder};
pub use error::SdkError;
pub use feed::{
    Feed, FeedEvent, FeedMessage, FeedPage, FeedReader, FeedReaderPage, FeedWriter, Subscription,
};
pub use member::{ClaimedWork, Member, WorkPage};
pub use model::{AppendOutcome, FetchPage, Record, TemporaryFetch, TemporaryPage, TransportLimits};
pub use pipe::{Pipe, PipeInfo};
pub use reader::Reader;
pub use store::{
    GetByPage, IteratePage, LookupOutcome, MutationOutcome, ReadFreshness, StateStore, StoreRow,
    SyncOutcome,
};
pub use transport::{HttpTransport, Transport, TransportReply};
pub use typed::{IndexBound, IndexPage, IndexQuery, TypedValue};
pub use typed_store::{IterateRows, Lookup, Row, RowPage, Store};
pub use writer::Writer;
