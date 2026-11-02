//! Binary fetch decode: the `application/vnd.whitewater.frames` page
//! envelope plus the stored record frame codec (`FSR1`/`FSR2`, with the
//! `FSR3` LZ4 envelope). Mirrors `src/codec.rs` and the fetch encoder in
//! the server — keep field order and integer widths identical.

use std::collections::BTreeMap;

use bytes::Buf;

use uuid::Uuid;

use crate::error::SdkError;
use crate::model::Record;

const MAGIC_V1: &[u8; 4] = b"FSR1";
const MAGIC_V2: &[u8; 4] = b"FSR2";
const MAGIC_LZ4: &[u8; 4] = b"FSR3";
/// Server `MAX_FRAME_BYTES` — a page announcing a larger frame is
/// corrupt, never a legitimate delivery.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// Server-side fetch limit — an announced record count beyond it is
/// corrupt, not a bounded page.
const MAX_PAGE_RECORDS: u32 = 65_536;

fn corrupt(detail: &str) -> SdkError {
    SdkError::Protocol(format!("binary fetch page corrupt: {detail}"))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize, what: &str) -> Result<&'a [u8], SdkError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| corrupt(what))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| corrupt(what))?;
        self.position = end;
        Ok(value)
    }

    fn le_u32(&mut self, what: &str) -> Result<u32, SdkError> {
        Ok(u32::from_le_bytes(
            self.take(4, what)?.try_into().map_err(|_| corrupt(what))?,
        ))
    }

    fn be_u16(&mut self, what: &str) -> Result<u16, SdkError> {
        Ok(u16::from_be_bytes(
            self.take(2, what)?.try_into().map_err(|_| corrupt(what))?,
        ))
    }

    fn be_u32(&mut self, what: &str) -> Result<u32, SdkError> {
        Ok(u32::from_be_bytes(
            self.take(4, what)?.try_into().map_err(|_| corrupt(what))?,
        ))
    }

    fn be_u64(&mut self, what: &str) -> Result<u64, SdkError> {
        Ok(u64::from_be_bytes(
            self.take(8, what)?.try_into().map_err(|_| corrupt(what))?,
        ))
    }

    fn be_i64(&mut self, what: &str) -> Result<i64, SdkError> {
        Ok(i64::from_be_bytes(
            self.take(8, what)?.try_into().map_err(|_| corrupt(what))?,
        ))
    }
}

/// One stored frame: `u32be body_len + body + u32be crc32(body)`.
/// Returns the decoded record fields; the caller attaches the page's
/// per-entry cursor string.
fn decode_frame(frame: &[u8]) -> Result<Record, SdkError> {
    if frame.len() < 8 {
        return Err(corrupt("truncated frame header"));
    }
    let body_len = u32::from_be_bytes(frame[..4].try_into().expect("u32 slice")) as usize;
    if body_len > MAX_FRAME_BYTES || frame.len() != body_len + 8 {
        return Err(corrupt("frame length mismatch"));
    }
    let body = &frame[4..4 + body_len];
    let checksum = u32::from_be_bytes(
        frame[4 + body_len..]
            .try_into()
            .map_err(|_| corrupt("truncated checksum"))?,
    );
    if crc32fast::hash(body) != checksum {
        return Err(corrupt("frame checksum mismatch"));
    }
    if body.starts_with(MAGIC_LZ4) {
        let mut envelope = Cursor::new(body);
        envelope.take(4, "lz4 magic")?;
        let decoded_len = envelope.be_u32("lz4 length")? as usize;
        if decoded_len > MAX_FRAME_BYTES {
            return Err(corrupt("lz4 decoded length exceeds bound"));
        }
        let mut decoded = vec![0; decoded_len];
        let actual = lz4_flex::block::decompress_into(&body[8..], &mut decoded)
            .map_err(|_| corrupt("lz4 decompress failed"))?;
        if actual != decoded_len || !decoded.starts_with(MAGIC_V2) {
            return Err(corrupt("lz4 payload is not a record frame"));
        }
        return decode_body(&decoded);
    }
    decode_body(body)
}

fn decode_body(body: &[u8]) -> Result<Record, SdkError> {
    let mut reader = Cursor::new(body);
    let magic = reader.take(4, "frame magic")?;
    if magic != MAGIC_V1 && magic != MAGIC_V2 {
        return Err(corrupt("unknown frame magic"));
    }
    let message_id =
        Uuid::from_slice(reader.take(16, "message id")?).map_err(|_| corrupt("message id"))?;
    // producer identity/sequence are dedup internals — not part of the
    // delivered Record model.
    reader.take(16, "producer id")?;
    reader.be_u64("producer sequence")?;
    let (event_time_ns, ingest_time_ns) = if magic == MAGIC_V1 {
        let timestamp_ms = reader.be_u64("legacy timestamp")?;
        let timestamp_ns =
            i64::try_from(timestamp_ms.saturating_mul(1_000_000)).unwrap_or(i64::MAX);
        (timestamp_ns, timestamp_ns)
    } else {
        (
            reader.be_i64("event_time_ns")?,
            reader.be_i64("ingest_time_ns")?,
        )
    };
    let key_len = reader.be_u32("key length")? as usize;
    let payload_len = reader.be_u32("payload length")? as usize;
    let metadata_count = reader.be_u16("metadata count")?;
    let key = reader.take(key_len, "key")?.to_vec();
    let payload = reader.take(payload_len, "payload")?.to_vec();
    let mut metadata = BTreeMap::new();
    for _ in 0..metadata_count {
        let name_len = reader.be_u16("metadata name length")? as usize;
        let value_len = reader.be_u32("metadata value length")? as usize;
        let name = std::str::from_utf8(reader.take(name_len, "metadata name")?)
            .map_err(|_| corrupt("metadata name utf8"))?
            .to_owned();
        let value = reader.take(value_len, "metadata value")?.to_vec();
        metadata.insert(name, value);
    }
    if reader.position != body.len() {
        return Err(corrupt("trailing bytes in frame"));
    }
    Ok(Record {
        message_id: Some(message_id),
        key,
        payload,
        metadata,
        event_time_ns,
        ingest_time_ns,
        cursor: None,
    })
}

/// Parse one binary fetch page: `u32le count`, `u32le next_cursor_len` +
/// cursor, then per record `u32le cursor_len` + cursor + `u32le
/// frame_len` + stored frame bytes.
pub(crate) fn parse_frame_page(bytes: &[u8]) -> Result<(Vec<Record>, Option<String>), SdkError> {
    let mut reader = Cursor::new(bytes);
    let count = reader.le_u32("record count")?;
    if count > MAX_PAGE_RECORDS {
        return Err(corrupt("record count exceeds page bound"));
    }
    let next_cursor_len = reader.le_u32("next cursor length")? as usize;
    let next_cursor = reader
        .take(next_cursor_len, "next cursor")
        .and_then(|raw| std::str::from_utf8(raw).map_err(|_| corrupt("next cursor utf8")))?;
    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let cursor_len = reader.le_u32("record cursor length")? as usize;
        let cursor = reader
            .take(cursor_len, "record cursor")
            .and_then(|raw| std::str::from_utf8(raw).map_err(|_| corrupt("record cursor utf8")))?
            .to_owned();
        let frame_len = reader.le_u32("frame length")? as usize;
        if frame_len > MAX_FRAME_BYTES + 8 {
            return Err(corrupt("frame length exceeds bound"));
        }
        let frame = reader.take(frame_len, "frame")?;
        let mut record = decode_frame(frame)?;
        record.cursor = Some(cursor);
        records.push(record);
    }
    if reader.position != bytes.len() {
        return Err(corrupt("trailing bytes in page"));
    }
    Ok((
        records,
        (!next_cursor.is_empty()).then(|| next_cursor.to_owned()),
    ))
}

/// Incremental [`parse_frame_page`]: feeds the same page envelope one
/// arbitrary chunk at a time and decodes each record as soon as its
/// bytes complete, so decode overlaps the transfer instead of waiting
/// for the whole body. `finish` enforces the same fail-closed contract
/// as the whole-buffer parse: exact completion, no trailing bytes.
pub(crate) struct FramePageParser {
    buffer: bytes::BytesMut,
    stage: FramePageStage,
    count: u32,
    next_cursor: Vec<u8>,
    pending_cursor: String,
    records: Vec<Record>,
}

#[derive(Clone)]
enum FramePageStage {
    Count,
    NextCursorLen,
    NextCursor(usize),
    CursorLen,
    Cursor(usize),
    FrameLen,
    Frame(usize),
    Done,
}

impl FramePageParser {
    pub(crate) fn new() -> Self {
        Self {
            buffer: bytes::BytesMut::new(),
            stage: FramePageStage::Count,
            count: 0,
            next_cursor: Vec::new(),
            pending_cursor: String::new(),
            records: Vec::new(),
        }
    }

    /// The page header (count + next_cursor) has been fully parsed:
    /// callers can already launch the cursor-chained next request while
    /// record frames are still streaming.
    pub(crate) fn header_ready(&self) -> bool {
        !matches!(
            self.stage,
            FramePageStage::Count | FramePageStage::NextCursorLen | FramePageStage::NextCursor(_)
        )
    }

    /// The parsed `next_cursor` once [`header_ready`](Self::header_ready)
    /// is true (`None` for an empty cursor).
    pub(crate) fn next_cursor(&self) -> Option<String> {
        String::from_utf8(self.next_cursor.clone())
            .ok()
            .filter(|cursor| !cursor.is_empty())
    }

    /// Feed one network chunk; any boundary — inside the page header,
    /// a record header, or a frame — is legal.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<(), SdkError> {
        self.buffer.extend_from_slice(chunk);
        self.drain()
    }

    /// The stream ended: the page must have completed exactly, with no
    /// partial field or trailing byte.
    pub(crate) fn finish(mut self) -> Result<(Vec<Record>, Option<String>), SdkError> {
        self.drain()?;
        match self.stage {
            FramePageStage::Done if self.buffer.is_empty() => {
                let next_cursor = std::str::from_utf8(&self.next_cursor)
                    .map_err(|_| corrupt("next cursor utf8"))?;
                Ok((
                    self.records,
                    (!next_cursor.is_empty()).then(|| next_cursor.to_owned()),
                ))
            }
            FramePageStage::Done => Err(corrupt("trailing bytes in page")),
            _ => Err(corrupt("truncated page")),
        }
    }

    fn drain(&mut self) -> Result<(), SdkError> {
        loop {
            match self.stage.clone() {
                FramePageStage::Count => {
                    if self.buffer.len() < 4 {
                        return Ok(());
                    }
                    let count = u32::from_le_bytes(self.buffer[..4].try_into().expect("u32"));
                    if count > MAX_PAGE_RECORDS {
                        return Err(corrupt("record count exceeds page bound"));
                    }
                    self.count = count;
                    self.records.reserve(count as usize);
                    self.buffer.advance(4);
                    self.stage = FramePageStage::NextCursorLen;
                }
                FramePageStage::NextCursorLen => {
                    if self.buffer.len() < 4 {
                        return Ok(());
                    }
                    let len =
                        u32::from_le_bytes(self.buffer[..4].try_into().expect("u32")) as usize;
                    self.buffer.advance(4);
                    self.stage = FramePageStage::NextCursor(len);
                }
                FramePageStage::NextCursor(len) => {
                    if self.buffer.len() < len {
                        return Ok(());
                    }
                    let raw = self.buffer.split_to(len);
                    std::str::from_utf8(&raw).map_err(|_| corrupt("next cursor utf8"))?;
                    self.next_cursor = raw.to_vec();
                    self.stage = FramePageStage::CursorLen;
                }
                FramePageStage::CursorLen => {
                    if self.records.len() == self.count as usize {
                        self.stage = FramePageStage::Done;
                        continue;
                    }
                    if self.buffer.len() < 4 {
                        return Ok(());
                    }
                    let len =
                        u32::from_le_bytes(self.buffer[..4].try_into().expect("u32")) as usize;
                    self.buffer.advance(4);
                    self.stage = FramePageStage::Cursor(len);
                }
                FramePageStage::Cursor(len) => {
                    if self.buffer.len() < len {
                        return Ok(());
                    }
                    let raw = self.buffer.split_to(len);
                    self.pending_cursor = std::str::from_utf8(&raw)
                        .map_err(|_| corrupt("record cursor utf8"))?
                        .to_owned();
                    self.stage = FramePageStage::FrameLen;
                }
                FramePageStage::FrameLen => {
                    if self.buffer.len() < 4 {
                        return Ok(());
                    }
                    let len =
                        u32::from_le_bytes(self.buffer[..4].try_into().expect("u32")) as usize;
                    if len > MAX_FRAME_BYTES + 8 {
                        return Err(corrupt("frame length exceeds bound"));
                    }
                    self.buffer.advance(4);
                    self.stage = FramePageStage::Frame(len);
                }
                FramePageStage::Frame(len) => {
                    if self.buffer.len() < len {
                        return Ok(());
                    }
                    let frame = self.buffer.split_to(len);
                    let mut record = decode_frame(&frame)?;
                    record.cursor = Some(std::mem::take(&mut self.pending_cursor));
                    self.records.push(record);
                    self.stage = FramePageStage::CursorLen;
                }
                FramePageStage::Done => return Ok(()),
            }
        }
    }
}

/// Client-side `FSA1` append record — the binary form of one
/// `WriterSessionAppendRequest` (server mints identity/sequence/
/// ingest-time downstream).
pub struct AppendFrame<'a> {
    pub writer: &'a str,
    pub request_id: uuid::Uuid,
    pub session_epoch: u64,
    pub event_time_ns: Option<i64>,
    pub use_compression: bool,
    pub key: &'a [u8],
    pub payload: &'a [u8],
    pub metadata: &'a std::collections::BTreeMap<String, Vec<u8>>,
}

/// Encode an append batch as an `application/vnd.whitewater.frames`
/// body: `u32 count` then `u32 body_len | body | u32 crc32(body)`
/// frames — big-endian, mirroring the stored-codec envelope.
pub fn encode_append_page(records: &[AppendFrame<'_>]) -> Vec<u8> {
    let mut page = Vec::with_capacity(4 + records.len() * 32);
    page.extend_from_slice(&(records.len() as u32).to_be_bytes());
    for record in records {
        let mut body = Vec::with_capacity(64 + record.key.len() + record.payload.len());
        body.extend_from_slice(b"FSA1");
        body.extend_from_slice(&(record.writer.len() as u16).to_be_bytes());
        body.extend_from_slice(record.writer.as_bytes());
        body.extend_from_slice(record.request_id.as_bytes());
        body.extend_from_slice(&record.session_epoch.to_be_bytes());
        body.push(u8::from(record.event_time_ns.is_some()) | u8::from(record.use_compression) << 1);
        if let Some(event_time_ns) = record.event_time_ns {
            body.extend_from_slice(&event_time_ns.to_be_bytes());
        }
        body.extend_from_slice(&(record.key.len() as u32).to_be_bytes());
        body.extend_from_slice(record.key);
        body.extend_from_slice(&(record.payload.len() as u32).to_be_bytes());
        body.extend_from_slice(record.payload);
        body.extend_from_slice(&(record.metadata.len() as u16).to_be_bytes());
        for (name, value) in record.metadata {
            body.extend_from_slice(&(name.len() as u16).to_be_bytes());
            body.extend_from_slice(&(value.len() as u32).to_be_bytes());
            body.extend_from_slice(name.as_bytes());
            body.extend_from_slice(value);
        }
        page.extend_from_slice(&(body.len() as u32).to_be_bytes());
        page.extend_from_slice(&body);
        page.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    }
    page
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal FSR2 encoder for tests — the production path only ever
    /// decodes server-produced frames.
    fn encode_v2(
        key: &[u8],
        payload: &[u8],
        metadata: &BTreeMap<String, Vec<u8>>,
        event_time_ns: i64,
        ingest_time_ns: i64,
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(MAGIC_V2);
        body.extend_from_slice(Uuid::nil().as_bytes());
        body.extend_from_slice(Uuid::nil().as_bytes());
        body.extend_from_slice(&7u64.to_be_bytes());
        body.extend_from_slice(&event_time_ns.to_be_bytes());
        body.extend_from_slice(&ingest_time_ns.to_be_bytes());
        body.extend_from_slice(&(key.len() as u32).to_be_bytes());
        body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        body.extend_from_slice(&(metadata.len() as u16).to_be_bytes());
        body.extend_from_slice(key);
        body.extend_from_slice(payload);
        for (name, value) in metadata {
            body.extend_from_slice(&(name.len() as u16).to_be_bytes());
            body.extend_from_slice(&(value.len() as u32).to_be_bytes());
            body.extend_from_slice(name.as_bytes());
            body.extend_from_slice(value);
        }
        let mut frame = Vec::new();
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        frame.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
        frame
    }

    fn encode_page(records: &[(String, Vec<u8>)], next_cursor: Option<&str>) -> Vec<u8> {
        let mut page = Vec::new();
        page.extend_from_slice(&(records.len() as u32).to_le_bytes());
        let cursor = next_cursor.unwrap_or_default();
        page.extend_from_slice(&(cursor.len() as u32).to_le_bytes());
        page.extend_from_slice(cursor.as_bytes());
        for (cursor, frame) in records {
            page.extend_from_slice(&(cursor.len() as u32).to_le_bytes());
            page.extend_from_slice(cursor.as_bytes());
            page.extend_from_slice(&(frame.len() as u32).to_le_bytes());
            page.extend_from_slice(frame);
        }
        page
    }

    #[test]
    fn decode_v2_frame_round_trips_fields() {
        let mut metadata = BTreeMap::new();
        metadata.insert("source".to_owned(), b"bench".to_vec());
        let frame = encode_v2(b"k1", b"payload-bytes", &metadata, 11, 22);
        let record = decode_frame(&frame).expect("decodes");
        assert_eq!(record.key, b"k1");
        assert_eq!(record.payload, b"payload-bytes");
        assert_eq!(record.metadata["source"], b"bench");
        assert_eq!(record.event_time_ns, 11);
        assert_eq!(record.ingest_time_ns, 22);
        assert_eq!(record.message_id, Some(Uuid::nil()));
    }

    #[test]
    fn decode_lz4_envelope_round_trips() {
        let plain = encode_v2(b"k", b"big-payload", &BTreeMap::new(), 1, 2);
        let body = &plain[4..plain.len() - 4];
        let compressed = lz4_flex::block::compress(body);
        let mut envelope_body = Vec::new();
        envelope_body.extend_from_slice(MAGIC_LZ4);
        envelope_body.extend_from_slice(&(body.len() as u32).to_be_bytes());
        envelope_body.extend_from_slice(&compressed);
        let mut frame = Vec::new();
        frame.extend_from_slice(&(envelope_body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&envelope_body);
        frame.extend_from_slice(&crc32fast::hash(&envelope_body).to_be_bytes());
        let record = decode_frame(&frame).expect("decodes");
        assert_eq!(record.payload, b"big-payload");
        assert_eq!(record.event_time_ns, 1);
    }

    #[test]
    fn parse_page_extracts_records_and_next_cursor() {
        let frame_a = encode_v2(b"ka", b"pa", &BTreeMap::new(), 1, 1);
        let frame_b = encode_v2(b"kb", b"pb", &BTreeMap::new(), 2, 2);
        let page = encode_page(
            &[("c1".to_owned(), frame_a), ("c2".to_owned(), frame_b)],
            Some("c2"),
        );
        let (records, next) = parse_frame_page(&page).expect("parses");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].cursor.as_deref(), Some("c1"));
        assert_eq!(records[1].cursor.as_deref(), Some("c2"));
        assert_eq!(records[1].payload, b"pb");
        assert_eq!(next.as_deref(), Some("c2"));
    }

    #[test]
    fn parse_page_rejects_corruption() {
        let mut bad = encode_v2(b"k", b"p", &BTreeMap::new(), 1, 1);
        *bad.last_mut().expect("nonempty") ^= 0xFF;
        let page = encode_page(&[("c".to_owned(), bad)], None);
        assert!(parse_frame_page(&page).is_err());
        assert!(parse_frame_page(&[]).is_err());

        let mut truncated = encode_page(
            &[(
                "c".to_owned(),
                encode_v2(b"k", b"p", &BTreeMap::new(), 1, 1),
            )],
            None,
        );
        truncated.truncate(truncated.len() - 3);
        assert!(parse_frame_page(&truncated).is_err());
    }

    #[test]
    fn incremental_parse_matches_whole_buffer_at_every_boundary() {
        let records = vec![
            (
                "c-1".to_owned(),
                encode_v2(
                    b"key-1",
                    b"payload-1",
                    &BTreeMap::from([("a".to_owned(), b"1".to_vec())]),
                    11,
                    12,
                ),
            ),
            (
                "c-2".to_owned(),
                encode_v2(b"key-2", &vec![7u8; 300], &BTreeMap::new(), 21, 22),
            ),
            (
                "c-3".to_owned(),
                encode_v2(b"k", b"", &BTreeMap::new(), 31, 32),
            ),
        ];
        let page = encode_page(&records, Some("cursor-9"));
        let (expected_records, expected_next) = parse_frame_page(&page).expect("page");
        for split in [1usize, 3, 7, 64, 4096] {
            let mut parser = FramePageParser::new();
            for chunk in page.chunks(split) {
                parser.feed(chunk).expect("feed");
            }
            let (records, next) = parser.finish().expect("finish");
            assert_eq!(records, expected_records, "split {split}");
            assert_eq!(next, expected_next, "split {split}");
        }
        // Corrupt + truncated + trailing streams all fail closed.
        let mut torn = page.clone();
        let mid = torn.len() / 2;
        torn[mid] ^= 0xFF;
        let mut parser = FramePageParser::new();
        let outcome = parser.feed(&torn).and_then(|_| parser.finish()).map(|_| ());
        assert!(outcome.is_err());
        let mut parser = FramePageParser::new();
        parser.feed(&page[..page.len() - 3]).expect("feed");
        assert!(parser.finish().is_err());
        let mut parser = FramePageParser::new();
        parser.feed(&page).expect("feed");
        parser.feed(&[0xAA]).expect("extra");
        assert!(parser.finish().is_err());
    }
}
