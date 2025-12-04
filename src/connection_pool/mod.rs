mod guard;

pub(crate) use crate::connection_pool::guard::ConnectionGuard;
use crate::constant::{CONNECTION_LIMIT, allocate_bytes};
use bytes::BytesMut;
use crossbeam::queue::SegQueue;
use std::sync::OnceLock;
use tokio::sync::{Semaphore, TryAcquireError};

struct ConnectionPool {
    buffer_pool: SegQueue<BytesMut>,
    semaphore: Semaphore,
}

impl ConnectionPool {
    fn new(size: u16) -> Self {
        let buffer_pool = SegQueue::new();

        for _ in 0..size {
            buffer_pool.push(allocate_bytes());
        }

        Self {
            buffer_pool,
            semaphore: Semaphore::new(size as usize),
        }
    }
}

static POOL: OnceLock<ConnectionPool> = OnceLock::new();

fn get_pool() -> &'static ConnectionPool {
    POOL.get_or_init(|| ConnectionPool::new(CONNECTION_LIMIT))
}

pub fn acquire_connection() -> Result<ConnectionGuard, TryAcquireError> {
    let permit = get_pool().semaphore.try_acquire()?;

    let bytes = get_pool().buffer_pool.pop().unwrap();

    Ok(ConnectionGuard::new(bytes, permit))
}
