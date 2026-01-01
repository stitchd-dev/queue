//! Application state management and queue lifecycle.
//!
//! This module manages the global application state, including the collection
//! of active queues and their periodic refresh from the database.

use crate::error::InsertionError;
use crate::queue::Queue;
use arc_swap::ArcSwap;
use deadpool_postgres::{Pool, PoolError};
use derive_more::{Display, Error, From};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Error type for application state operations.
#[derive(From, Error, Debug, Display)]
pub enum AppStateError {
    /// Database connection pool error.
    Pool(PoolError),
    /// Database query error.
    Query(tokio_postgres::Error),
}

/// Global application state containing all active queues.
///
/// The state maintains a collection of queues and automatically refreshes
/// them from the database at regular intervals.
pub struct AppState {
    /// Map of queue IDs to queue instances.
    queues: Arc<ArcSwap<HashMap<i32, Arc<Queue>>>>,
    /// Background task handle for periodic queue refresh.
    _refresh_handle: JoinHandle<()>,
}

impl AppState {
    /// Checks if a queue with the given ID exists in the state.
    pub fn check_if_queue_exists(&self, queue_id: i32) -> bool {
        self.queues.load().contains_key(&queue_id)
    }

    /// Initializes the application state and starts the queue refresh loop.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool.
    /// * `queue_refresh_delay` - Interval between queue refreshes.
    /// * `max_buffer_size` - Maximum buffer size for each queue.
    /// * `max_buffer_duration` - Maximum duration before auto-sync.
    /// * `max_events_per_dataset` - Maximum events per dataset.
    ///
    /// # Returns
    /// An `Arc<AppState>` on success, or an error if initialization fails.
    pub async fn start(
        pool: Pool,
        queue_refresh_delay: Duration,
        max_buffer_size: u8,
        max_buffer_duration: Duration,
        max_events_per_dataset: u32,
    ) -> Result<Arc<Self>, AppStateError> {
        let queues = Arc::new(ArcSwap::from_pointee(HashMap::new()));
        Self::refresh_queues(
            &pool,
            queues.clone(),
            max_buffer_size,
            max_buffer_duration,
            max_events_per_dataset,
        )
        .await?;

        let queues_clone = queues.clone();

        let max_buffer_size_clone = max_buffer_size;
        let max_buffer_duration_clone = max_buffer_duration;
        let max_events_per_dataset_clone = max_events_per_dataset;

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

    /// Inserts data into the specified queue.
    ///
    /// # Arguments
    /// * `queue_id` - The ID of the target queue.
    /// * `data` - Vector of JSON values to insert.
    ///
    /// # Errors
    /// Returns `InsertionError::QueueNotFound` if the queue doesn't exist.
    pub async fn insert_data(&self, queue_id: i32, data: Vec<Vec<u8>>) -> Result<(), InsertionError> {
        let queue = self
            .queues
            .load()
            .get(&queue_id)
            .cloned()
            .ok_or(InsertionError::QueueNotFound(queue_id))?;

        queue.insert_data(data).await
    }

    /// Refreshes the queue collection from the database.
    ///
    /// Queries the database for active queues and updates the in-memory collection:
    /// - Removes queues that are no longer active.
    /// - Adds newly activated queues.
    async fn refresh_queues(
        pool_clone: &Pool,
        queues_clone: Arc<ArcSwap<HashMap<i32, Arc<Queue>>>>,
        max_buffer_size: u8,
        max_buffer_duration: Duration,
        max_events_per_dataset: u32,
    ) -> Result<(), AppStateError> {
        let queues = Self::get_queues(pool_clone).await?;

        tracing::debug!("Queues are {:?}", queues);

        let current_queues = queues_clone
            .load()
            .keys()
            .cloned()
            .collect::<HashSet<i32>>();

        let to_be_removed: Vec<i32> = current_queues.difference(&queues).cloned().collect();
        let to_be_added: Vec<i32> = queues.difference(&current_queues).cloned().collect();

        if !(to_be_removed.is_empty() && to_be_added.is_empty()) {
            let mut new_map = (**queues_clone.load()).clone();

            for queue_id in to_be_removed {
                new_map.remove(&queue_id);
            }

            for queue_id in to_be_added {
                new_map.insert(
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

            queues_clone.store(Arc::new(new_map));
        }

        Ok(())
    }

    /// Fetches the set of active queue IDs from the database.
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
