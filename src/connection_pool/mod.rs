//! Connection and process pooling for resource management.
//!
//! This module provides two types of pools:
//! - **Process Pool**: Limits concurrent in-flight processing requests.
//! - **Connection Pool**: Manages reusable byte buffers for TCP connections.

mod guard;

pub(crate) use crate::connection_pool::guard::ConnectionGuard;
use crate::constant::{CONNECTION_LIMIT, IN_FLIGHT_LIMIT, allocate_bytes};
use crate::health::PoolState;
use bytes::BytesMut;
use crossbeam::queue::SegQueue;
use std::sync::OnceLock;
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};

/// Global process pool semaphore for rate limiting.
static PROCESS_POOL: OnceLock<Semaphore> = OnceLock::new();

/// Returns a reference to the process pool semaphore.
fn process_pool() -> &'static Semaphore {
    PROCESS_POOL.get_or_init(|| Semaphore::new(IN_FLIGHT_LIMIT))
}

/// Attempts to acquire a processing permit.
///
/// # Returns
/// A permit on success, or `TryAcquireError` if the limit is reached.
pub fn acquire_process() -> Result<SemaphorePermit<'static>, TryAcquireError> {
    PROCESS_POOL
        .get_or_init(|| Semaphore::new(IN_FLIGHT_LIMIT))
        .try_acquire()
}

/// Returns the current health status of the process pool.
pub fn get_process_pool_health() -> PoolState {
    PoolState {
        active: process_pool().available_permits(),
        total: IN_FLIGHT_LIMIT,
    }
}

/// Internal connection pool managing byte buffers and connection permits.
struct ConnectionPool {
    /// Queue of reusable byte buffers.
    buffer_pool: SegQueue<BytesMut>,
    /// Semaphore limiting concurrent connections.
    semaphore: Semaphore,
}

impl ConnectionPool {
    /// Creates a new connection pool with the specified size.
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

/// Global connection pool instance.
static POOL: OnceLock<ConnectionPool> = OnceLock::new();

/// Returns a reference to the global connection pool.
fn get_pool() -> &'static ConnectionPool {
    POOL.get_or_init(|| ConnectionPool::new(CONNECTION_LIMIT))
}

/// Returns the current health status of the connection pool.
pub fn get_connection_pool_health() -> PoolState {
    PoolState {
        active: get_pool().semaphore.available_permits(),
        total: CONNECTION_LIMIT as usize,
    }
}

/// Attempts to acquire a connection with a byte buffer.
///
/// # Returns
/// A `ConnectionGuard` on success, or `TryAcquireError` if:
/// - The connection limit is reached.
/// - The buffer pool is out of sync (fatal error).
pub fn acquire_connection() -> Result<ConnectionGuard, TryAcquireError> {
    let permit = get_pool().semaphore.try_acquire()?;

    match get_pool().buffer_pool.pop() {
        Some(bytes) => Ok(ConnectionGuard::new(bytes, permit)),
        None => {
            tracing::error!("FATAL: Bytes Buffer Pool is out of sync from Connection Pool");

            Err(TryAcquireError::Closed)
        }
    }
}
