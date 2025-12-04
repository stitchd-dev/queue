use std::sync::OnceLock;
use tokio::sync::Semaphore;

const fn const_min(a: usize, b: usize) -> usize {
    if a < b { a } else { b }
}

static IN_FLIGHT_LIMIT: OnceLock<Semaphore> = OnceLock::new();
pub fn inflight_limit() -> &'static Semaphore {
    IN_FLIGHT_LIMIT.get_or_init(|| Semaphore::new(200))
}

static CONNECTION_LIMIT: OnceLock<Semaphore> = OnceLock::new();
pub fn connection_limit() -> &'static Semaphore {
    CONNECTION_LIMIT.get_or_init(|| Semaphore::new(100))
}

pub const PING_MATCH: &[u8; 4] = b"ping";
pub const INSERT_MATCH: &[u8; 7] = b"insert ";
pub const MIN_SIZE: usize = const_min(PING_MATCH.len(), INSERT_MATCH.len());

pub fn is_ping(bytes: &[u8]) -> bool {
    bytes.starts_with(PING_MATCH)
}

pub fn is_insert(bytes: &[u8]) -> Option<&[u8]> {
    bytes.strip_prefix(INSERT_MATCH)
}

pub fn min_len_check(bytes: &[u8]) -> bool {
    bytes.len() >= MIN_SIZE
}

pub const READ_CHUNK_SIZE: usize = 64 * 1024;
pub const MAX_MESSAGE_SIZE: u64 = 1024 * 1024;
