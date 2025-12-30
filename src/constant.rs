//! Constants and utility functions for message parsing and validation.
//!
//! This module defines protocol constants, size limits, and helper functions
//! for parsing and validating client messages.

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;

/// Maximum total message size (2 MB).
pub const MAX_MESSAGE_SIZE: usize = 2 * 1024 * 1024;
/// Maximum message size as u64 for read operations.
const MAX_MESSAGE_SIZE_U64: u64 = MAX_MESSAGE_SIZE as u64;

/// Maximum number of concurrent in-flight processing requests.
pub(crate) const IN_FLIGHT_LIMIT: usize = 200;
/// Maximum number of concurrent TCP connections.
pub(crate) const CONNECTION_LIMIT: u16 = 20;

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
