use bytes::BytesMut;
use std::sync::OnceLock;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::Semaphore;

const PING_MATCH: &[u8; 4] = b"ping";
const INSERT_MATCH: &[u8; 7] = b"insert ";
const MIN_MESSAGE_SIZE: usize = 4;
pub(crate) const IN_FLIGHT_LIMIT: usize = 200;
pub(crate) const CONNECTION_LIMIT: u16 = 100;
const READ_CHUNK_SIZE: usize = 64 * 1024;
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
const MAX_MESSAGE_SIZE_U64: u64 = MAX_MESSAGE_SIZE as u64;

static _IN_FLIGHT_LIMIT: OnceLock<Semaphore> = OnceLock::new();

pub fn in_flight_limit() -> &'static Semaphore {
    _IN_FLIGHT_LIMIT.get_or_init(|| Semaphore::new(IN_FLIGHT_LIMIT))
}

pub fn is_ping(bytes: &[u8]) -> bool {
    bytes.starts_with(PING_MATCH)
}

pub fn is_insert(bytes: &[u8]) -> Option<&[u8]> {
    bytes.strip_prefix(INSERT_MATCH)
}
pub fn min_len_check(bytes: &[u8]) -> bool {
    bytes.len() >= MIN_MESSAGE_SIZE
}

pub fn is_valid_chunk(chunk_size: usize) -> bool {
    chunk_size <= READ_CHUNK_SIZE
}

pub fn is_valid_message(message_size: usize) -> bool {
    message_size < MAX_MESSAGE_SIZE
}

pub fn allocate_bytes() -> BytesMut {
    BytesMut::with_capacity(MAX_MESSAGE_SIZE)
}

pub async fn read_data(
    reader: &mut BufReader<OwnedReadHalf>,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    reader.take(MAX_MESSAGE_SIZE_U64).read_buf(buf).await
}
