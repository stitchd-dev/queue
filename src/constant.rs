//! Constants and utility functions for message parsing and validation.
//!
//! This module defines protocol constants, size limits, and helper functions
//! for parsing and validating client messages.

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;

/// Byte pattern for ping command.
const PING_MATCH: &[u8; 4] = b"ping";
/// Byte pattern for insert command prefix.
const INSERT_MATCH: &[u8; 7] = b"insert ";
/// Minimum valid message size in bytes.
const MIN_MESSAGE_SIZE: usize = 4;
/// Maximum size for individual JSON chunks (64 KB).
const READ_CHUNK_SIZE: usize = 64 * 1024;
/// Maximum total message size (1 MB).
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
/// Maximum message size as u64 for read operations.
const MAX_MESSAGE_SIZE_U64: u64 = MAX_MESSAGE_SIZE as u64;

/// Maximum number of concurrent in-flight processing requests.
pub(crate) const IN_FLIGHT_LIMIT: usize = 200;
/// Maximum number of concurrent TCP connections.
pub(crate) const CONNECTION_LIMIT: u16 = 20;

/// Checks if the bytes represent a ping command.
pub(crate) fn is_ping(bytes: &[u8]) -> bool {
    bytes.starts_with(PING_MATCH)
}

/// Checks if the bytes represent an insert command and returns the payload.
///
/// Returns `Some(&[u8])` with the remaining bytes after the "insert " prefix,
/// or `None` if the bytes don't start with the insert command.
pub(crate) fn extract_payload_if_insert(bytes: &[u8]) -> Option<&[u8]> {
    bytes.strip_prefix(INSERT_MATCH)
}

/// Checks if the message meets the minimum length requirement.
pub(crate) fn min_len_check(bytes: &[u8]) -> bool {
    bytes.len() >= MIN_MESSAGE_SIZE
}

/// Validates that a JSON chunk size is within acceptable limits.
pub(crate) fn is_valid_chunk(chunk_size: usize) -> bool {
    chunk_size <= READ_CHUNK_SIZE
}

/// Validates that the total message size is within acceptable limits.
pub(crate) fn is_valid_message(message_size: usize) -> bool {
    message_size < MAX_MESSAGE_SIZE
}

/// Allocates a new BytesMut buffer with maximum message capacity.
pub(crate) fn allocate_bytes() -> BytesMut {
    BytesMut::with_capacity(MAX_MESSAGE_SIZE)
}

/// Reads data from a TCP stream into a buffer, respecting size limits.
///
/// # Arguments
/// * `reader` - The buffered TCP reader.
/// * `buf` - The buffer to read data into.
///
/// # Returns
/// The number of bytes read, or an I/O error.
pub(crate) async fn read_data(
    reader: &mut BufReader<OwnedReadHalf>,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    reader.take(MAX_MESSAGE_SIZE_U64).read_buf(buf).await
}
