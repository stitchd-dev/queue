//! Health check utilities for monitoring system resources.
//!
//! This module provides structures and functions to query the health status
//! of connection pools, processing pools, and the database connection pool.

use crate::connection_pool::{get_connection_pool_health, get_process_pool_health};
use deadpool::Status;

/// Represents the state of a resource pool.
pub struct PoolState {
    /// Number of active (available) permits in the pool.
    pub(crate) active: usize,
    /// Total capacity of the pool.
    pub(crate) total: usize,
}

/// Overall health status of the system.
pub struct Health {
    /// Connection pool health.
    pub connections: PoolState,
    /// Processing pool health.
    pub processors: PoolState,
    /// Database connection pool status.
    pub db_pool: Status,
}

impl Health {
    /// Retrieves the current health status of all system pools.
    ///
    /// # Arguments
    /// * `db_pool` - The PostgreSQL connection pool to check.
    ///
    /// # Returns
    /// A `Health` struct containing the status of all monitored resources.
    pub fn get_health(db_pool: deadpool_postgres::Pool) -> Health {
        let connections = get_connection_pool_health();
        let processors = get_process_pool_health();

        Health {
            connections,
            processors,
            db_pool: db_pool.status(),
        }
    }
}
