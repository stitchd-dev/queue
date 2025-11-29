//! Error types for queue operations.
//!
//! This module defines error types used throughout the event queue system:
//! - `InsertionError`: Errors that occur during data insertion into the queue buffer
//! - `SyncError`: Errors that occur during synchronization (flushing) to PostgreSQL

use deadpool_postgres::PoolError;
use derive_more::with_trait::{Display, Error, From};

/// Error type for data insertion operations.
///
/// Represents failures that can occur when attempting to insert data into the queue buffer.
#[derive(From, Debug, Display)]
pub enum InsertionError {
    /// The insertion size exceeds the maximum allowed.
    ///
    /// The contained `usize` value indicates the maximum size that is allowed.
    /// This prevents memory exhaustion from excessively large batch insertions.
    LimitExceeded(usize),
    
    /// Attempted to insert an empty data collection.
    ///
    /// The queue requires at least one item to insert. Empty insertions are rejected
    /// to avoid unnecessary operations.
    EmptyData,
}

impl Error for InsertionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}

/// Error type for synchronization (flush) operations.
///
/// Represents failures that can occur when flushing buffered data to PostgreSQL.
/// These errors typically indicate database connectivity issues or transaction failures.
#[derive(From, Debug, Display, Error)]
pub enum SyncError {
    /// Connection pool error.
    ///
    /// Occurs when unable to acquire a database connection from the pool.
    /// May indicate pool exhaustion or configuration issues.
    Pool(PoolError),
    
    /// PostgreSQL database error.
    ///
    /// Wraps errors from the underlying tokio-postgres driver, including
    /// query failures, transaction errors, and network issues.
    DbError(tokio_postgres::error::Error),
}
