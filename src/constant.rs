use bytes::BytesMut;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;

const PING_MATCH: &[u8; 4] = b"ping";
const INSERT_MATCH: &[u8; 7] = b"insert ";
const MIN_MESSAGE_SIZE: usize = 4;
const READ_CHUNK_SIZE: usize = 64 * 1024;
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
const MAX_MESSAGE_SIZE_U64: u64 = MAX_MESSAGE_SIZE as u64;

pub(crate) const IN_FLIGHT_LIMIT: usize = 200;
pub(crate) const EVENT_PROCESSOR_MPSC_BUFFER_LIMIT: usize = 100;
pub(crate) const CONNECTION_LIMIT: u16 = 20;

pub(crate) fn is_ping(bytes: &[u8]) -> bool {
    bytes.starts_with(PING_MATCH)
}

pub(crate) fn is_insert(bytes: &[u8]) -> Option<&[u8]> {
    bytes.strip_prefix(INSERT_MATCH)
}
pub(crate) fn min_len_check(bytes: &[u8]) -> bool {
    bytes.len() >= MIN_MESSAGE_SIZE
}

pub(crate) fn is_valid_chunk(chunk_size: usize) -> bool {
    chunk_size <= READ_CHUNK_SIZE
}

pub(crate) fn is_valid_message(message_size: usize) -> bool {
    message_size < MAX_MESSAGE_SIZE
}

pub(crate) fn allocate_bytes() -> BytesMut {
    BytesMut::with_capacity(MAX_MESSAGE_SIZE)
}

pub(crate) async fn read_data(
    reader: &mut BufReader<OwnedReadHalf>,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    reader.take(MAX_MESSAGE_SIZE_U64).read_buf(buf).await
}
