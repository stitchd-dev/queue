//! Error types for queue operations.

use deadpool_postgres::PoolError;
use derive_more::with_trait::{Display, Error, From};

/// Error type for data insertion operations.
#[derive(From, Debug, Display)]
pub enum InsertionError {
    /// Insertion size exceeds maximum allowed.
    LimitExceeded(usize),

    /// Attempted to insert empty data collection.
    EmptyData,
    /// Queue not Found
    QueueNotFound(i32)
}

impl Error for InsertionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}

/// Error type for synchronization (flush) operations.
#[derive(From, Debug, Display, Error)]
pub enum SyncError {
    /// Connection connection_pool error.
    Pool(PoolError),

    /// PostgreSQL database error.
    DbError(tokio_postgres::error::Error),
}
