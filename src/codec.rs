//! Value codecs: bind a serialization contract once and every Feed
//! read/write and StateStore get/put runs through it — callers never
//! hand-encode bytes on the typed path.
//!
//! Built-ins cover the common shapes; `Codec::custom` carries protobuf,
//! Avro, MessagePack, framing, or any schema-aware format without an SDK
//! change. The wire stays codec-neutral: a codec name is descriptive
//! only and never crosses the wire.

use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use crate::error::SdkError;

type EncodeFn<T> = Arc<dyn Fn(&T) -> Result<Vec<u8>, SdkError> + Send + Sync>;
type DecodeFn<T> = Arc<dyn Fn(&[u8]) -> Result<T, SdkError> + Send + Sync>;

/// One bidirectional value codec. `encode` produces the record/row
/// bytes; `decode` reverses them. Custom codecs map failures to
/// `SdkError::Codec` so callers get a typed, non-retryable signal —
/// a value that cannot serialize is never ambiguous server-side.
pub struct Codec<T> {
    name: String,
    encode: EncodeFn<T>,
    decode: DecodeFn<T>,
}

impl<T> Clone for Codec<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            encode: self.encode.clone(),
            decode: self.decode.clone(),
        }
    }
}

impl<T> Codec<T> {
    /// Wrap arbitrary encode/decode closures — the escape hatch for
    /// protobuf, Avro, MessagePack, or schema-registry-bound formats.
    pub fn custom<E, D>(name: impl Into<String>, encode: E, decode: D) -> Self
    where
        E: Fn(&T) -> Result<Vec<u8>, SdkError> + Send + Sync + 'static,
        D: Fn(&[u8]) -> Result<T, SdkError> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            encode: Arc::new(encode),
            decode: Arc::new(decode),
        }
    }

    /// Descriptive identity only — never negotiated on the wire.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn encode(&self, value: &T) -> Result<Vec<u8>, SdkError> {
        (self.encode)(value)
    }

    pub fn decode(&self, bytes: &[u8]) -> Result<T, SdkError> {
        (self.decode)(bytes)
    }
}

impl Codec<String> {
    /// UTF-8 text — the default key codec across the SDK family.
    pub fn utf8() -> Self {
        Self::custom(
            "utf8",
            |value| Ok(value.as_bytes().to_vec()),
            |bytes| {
                String::from_utf8(bytes.to_vec())
                    .map_err(|error| SdkError::Codec(format!("invalid utf-8 key: {error}")))
            },
        )
    }
}

impl Codec<Vec<u8>> {
    /// Raw bytes pass through unchanged — the default payload codec.
    pub fn bytes() -> Self {
        Self::custom(
            "bytes",
            |value| Ok(value.clone()),
            |bytes| Ok(bytes.to_vec()),
        )
    }
}

impl<T> Codec<T>
where
    T: Serialize + DeserializeOwned,
{
    /// `serde_json` object codec — the standard payload shape for
    /// structured records; wire bytes are the document's JSON text.
    pub fn json() -> Self {
        Self::custom(
            "json",
            |value| {
                serde_json::to_vec(value)
                    .map_err(|error| SdkError::Codec(format!("json encode: {error}")))
            },
            |bytes| {
                serde_json::from_slice(bytes)
                    .map_err(|error| SdkError::Codec(format!("json decode: {error}")))
            },
        )
    }
}
