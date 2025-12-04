//! Binary entrypoint used for quick local connectivity checks.
//!
//! This example sets up a Deadpool Postgres connection pool and performs a
//! simple `SELECT 1 + $1` query to verify database access. Application code
//! would normally create or obtain a `Queue` from the `queue` module and use
//! it to buffer and flush events.

mod command;
mod connection;
mod constant;
mod error;
pub mod queue;
mod state;

use crate::command::Command;
use crate::connection::listen;
use crate::state::AppState;
use deadpool::Runtime;
use deadpool_postgres::tokio_postgres::NoTls;
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Configure the Postgres pool. In production, prefer reading these from env vars.
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

    let state = create_app_state(pool.clone()).await;

    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();

    let (server_tx, worker_rx) = mpsc::channel::<Command>(500);

    let _worker_handle = connection::process(worker_rx, state);

    listen(listener, server_tx).await;
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
