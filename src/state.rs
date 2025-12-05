use crate::error::InsertionError;
use crate::queue::Queue;
use deadpool_postgres::{Pool, PoolError};
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
    Query(tokio_postgres::Error),
}

pub struct AppState {
    queues: Arc<RwLock<HashMap<i32, Arc<Queue>>>>,
    _refresh_handle: JoinHandle<()>,
}

impl AppState {
    pub async fn check_if_queue_exists(&self, queue_id: i32) -> bool {
        self.queues.read().await.contains_key(&queue_id)
    }

    pub async fn start(
        pool: Pool,
        queue_refresh_delay: Duration,
        max_buffer_size: u8,
        max_buffer_duration: Duration,
        max_events_per_dataset: u32,
    ) -> Result<Arc<Self>, AppStateError> {
        let queues = Arc::new(RwLock::new(HashMap::new()));
        Self::refresh_queues(
            &pool,
            queues.clone(),
            max_buffer_size,
            max_buffer_duration,
            max_events_per_dataset,
        )
        .await?;

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

                    if let Err(err) = Self::refresh_queues(
                        &pool,
                        queues,
                        max_buffer_size_clone,
                        max_buffer_duration_clone,
                        max_events_per_dataset_clone,
                    )
                    .await
                    {
                        tracing::error!("Failed to refresh queues: {}", err);
                    }
                }
            }),
        };

        Ok(Arc::new(state))
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
    ) -> Result<(), AppStateError> {
        let queues = Self::get_queues(&pool_clone).await?;

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

        Ok(())
    }

    async fn get_queues(pool: &Pool) -> Result<HashSet<i32>, AppStateError> {
        let conn = pool.get().await?;

        let queues = conn
            .query("SELECT id FROM queue WHERE active = true", &[])
            .await?;

        Ok(queues
            .iter()
            .map(|row| row.get(0))
            .collect::<HashSet<i32>>())
    }
}
