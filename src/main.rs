//! Binary entrypoint used for quick local connectivity checks.
//!
//! This example sets up a Deadpool Postgres connection pool and performs a
//! simple `SELECT 1 + $1` query to verify database access. Application code
//! would normally create or obtain a `Queue` from the `queue` module and use
//! it to buffer and flush events.

mod error;
pub mod queue;

use crate::error::InsertionError;
use crate::queue::Queue;
use deadpool::Runtime;
use deadpool_postgres::tokio_postgres::NoTls;
use deadpool_postgres::{Config, ManagerConfig, Pool, PoolError, RecyclingMethod};
use derive_more::{Display, Error, From};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

#[derive(From, Error, Debug, Display)]
pub enum AppStateError {
    Pool(PoolError),
}

pub struct AppState {
    queues: Arc<RwLock<HashMap<i32, Arc<Queue>>>>,
    _refresh_handle: JoinHandle<()>,
}

impl AppState {
    pub async fn start(
        pool: Pool,
        queue_refresh_delay: Duration,
        max_buffer_size: u8,
        max_buffer_duration: Duration,
        max_events_per_dataset: u32,
    ) -> Result<Self, AppStateError> {
        let queues = Arc::new(RwLock::new(HashMap::new()));
        Self::refresh_queues(
            &pool,
            queues.clone(),
            max_buffer_size,
            max_buffer_duration,
            max_events_per_dataset,
        )
        .await;

        let queues_clone = queues.clone();

        let max_buffer_size_clone = max_buffer_size.clone();
        let max_buffer_duration_clone = max_buffer_duration.clone();
        let max_events_per_dataset_clone = max_events_per_dataset.clone();

        let state = Self {
            queues,
            _refresh_handle: tokio::spawn(async move {
                loop {
                    tokio::time::sleep(queue_refresh_delay).await;

                    let queues = queues_clone.clone();
                    Self::refresh_queues(
                        &pool,
                        queues,
                        max_buffer_size_clone,
                        max_buffer_duration_clone,
                        max_events_per_dataset_clone,
                    )
                    .await;
                }
            }),
        };

        Ok(state)
    }

    pub async fn insert_data(&self, queue_id: i32, data: Vec<Value>) -> Result<(), InsertionError> {
        let queue = self
            .queues
            .read()
            .await
            .get(&queue_id)
            .cloned()
            .ok_or(InsertionError::QueueNotFound(queue_id))?;

        queue.insert_data(data).await
    }

    async fn refresh_queues(
        pool_clone: &Pool,
        queues_clone: Arc<RwLock<HashMap<i32, Arc<Queue>>>>,
        max_buffer_size: u8,
        max_buffer_duration: Duration,
        max_events_per_dataset: u32,
    ) {
        let queues = Self::get_queues(&pool_clone).await.unwrap();

        println!("Queues are {:?}", queues);

        let current_queues = queues_clone
            .read()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<i32>>();

        let to_be_removed: Vec<i32> = current_queues.difference(&queues).cloned().collect();
        let to_be_added: Vec<i32> = queues.difference(&current_queues).cloned().collect();

        if !(to_be_removed.is_empty() && to_be_added.is_empty()) {
            let mut write = queues_clone.write().await;

            for queue_id in to_be_removed {
                write.remove(&queue_id);
            }

            for queue_id in to_be_added {
                write.insert(
                    queue_id,
                    Arc::new(Queue::get_queue(
                        queue_id,
                        pool_clone.clone(),
                        Some(max_buffer_duration),
                        Some(max_buffer_size),
                        Some(max_events_per_dataset),
                    )),
                );
            }
        }
    }

    async fn get_queues(pool: &Pool) -> Result<HashSet<i32>, AppStateError> {
        let conn = pool.get().await?;

        let queues = conn
            .query("SELECT id FROM queue WHERE active = true", &[])
            .await
            .unwrap();

        Ok(queues
            .iter()
            .map(|row| row.get(0))
            .collect::<HashSet<i32>>())
    }
}

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

    ingest_data(state).await;
}

async fn create_app_state(pool: Pool) -> AppState {
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

async fn ingest_data(state: AppState) {
    let time = tokio::time::Instant::now();
    for i in 0..40000 {
        let data = (0..70)
            .map(|v| {
                serde_json::json!({
                    "a": i,
                    "b": v
                })
            })
            .collect::<Vec<_>>();

        state.insert_data(1, data).await.unwrap();
    }

    let elapsed = time.elapsed().as_millis();

    println!("Elapsed Time {elapsed} ms");
}
