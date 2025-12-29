//! Connection guard for managing byte buffers with RAII semantics.
//!
//! This module provides `ConnectionGuard`, which automatically returns
//! byte buffers to the pool when dropped.

use crate::connection_pool::get_pool;
use bytes::BytesMut;
use tokio::sync::SemaphorePermit;

/// RAII guard for a connection's byte buffer.
///
/// Holds a byte buffer and a semaphore permit. When dropped, the buffer
/// is cleared and returned to the pool, and the permit is released.
pub struct ConnectionGuard {
    /// The byte buffer for this connection.
    bytes: BytesMut,
    /// Semaphore permit that limits concurrent connections.
    _permit: SemaphorePermit<'static>,
}

impl ConnectionGuard {
    /// Returns a mutable reference to the byte buffer.
    pub fn bytes(&mut self) -> &mut BytesMut {
        &mut self.bytes
    }

    /// Creates a new `ConnectionGuard` with the given buffer and permit.
    pub(super) fn new(bytes: BytesMut, permit: SemaphorePermit<'static>) -> Self {
        Self {
            bytes,
            _permit: permit,
        }
    }
}

impl Drop for ConnectionGuard {
    /// Clears the buffer and returns it to the pool when the guard is dropped.
    fn drop(&mut self) {
        let mut bytes = std::mem::replace(&mut self.bytes, BytesMut::new());
        bytes.clear();
        get_pool().buffer_pool.push(bytes);
    }
}
