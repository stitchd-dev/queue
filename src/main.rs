//! Binary entrypoint used for quick local connectivity checks.
//!
//! This example sets up a Deadpool Postgres connection connection_pool and performs a
//! simple `SELECT 1 + $1` query to verify database access. Application code
//! would normally create or obtain a `Queue` from the `queue` module and use
//! it to buffer and flush events.

mod command;
mod connection;
pub(crate) mod connection_pool;
mod constant;
mod error;
pub mod health;
pub mod queue;
mod state;

use crate::connection::listen;
use crate::state::AppState;
use deadpool::Runtime;
use deadpool_postgres::tokio_postgres::NoTls;
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Configure the Postgres connection_pool. In production, prefer reading these from env vars.
    let mut config = Config::new();
    config.dbname = Some("postgres".to_string());
    config.user = Some("postgres".to_string());
    config.password = Some("password".to_string());
    config.host = Some("localhost".to_string());
    config.port = Some(5432);

    // Use fast recycling which avoids full connection reset where safe.
    config.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    let pool = config.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

    // TODO: Implement a periodic health metric check to check if the connection_pool is healthy.

    let state = create_app_state(pool.clone()).await;

    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();

    listen(listener, state).await;
}

async fn create_app_state(pool: Pool) -> Arc<AppState> {
    AppState::start(
        pool,
        Duration::from_secs(120),
        128,
        Duration::from_secs(2),
        100000,
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to initialize state: {}", e);
        e
    })
    .unwrap()
}
