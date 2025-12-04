use crate::connection_pool::get_pool;
use bytes::BytesMut;
use tokio::sync::SemaphorePermit;

pub struct ConnectionGuard {
    bytes: BytesMut,
    _permit: SemaphorePermit<'static>,
}

impl ConnectionGuard {
    pub fn bytes(&mut self) -> &mut BytesMut {
        &mut self.bytes
    }

    pub(super) fn new(bytes: BytesMut, permit: SemaphorePermit<'static>) -> Self {
        Self {
            bytes,
            _permit: permit,
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let mut bytes = std::mem::replace(&mut self.bytes, BytesMut::new());
        bytes.clear();
        get_pool().buffer_pool.push(bytes);
    }
}
