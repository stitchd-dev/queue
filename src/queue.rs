//! Queue module
//! 
//! This module implements an in-memory buffering queue that batches JSON payloads
//! and periodically flushes them into PostgreSQL using efficient binary `COPY`.
//! The queue supports two triggers for a flush ("sync"):
//! - size-based: when the number of buffered items exceeds `max_size`
//! - time-based: when `max_duration` elapses since the first item was added
//!
//! Concurrency model:
//! - Internal maps and state are protected by `tokio::sync::Mutex`.
//! - A single optional background task is used to schedule the time-based sync.
//! - All database interactions are executed in a single transaction per flush.
//!
//! Database expectations:
//! - A row exists in table `queue` with this queue's `destination_id`.
//! - Helper SQL functions exist: `get_current_dataset(queue_id, max_size, incoming_count)`
//!   that returns an integer dataset id, and `release_reservation(queue_id, dataset_id, count)`
//!   to finalize reservations after COPY.
//! - Physical partitioned tables are expected to exist and be named as:
//!   `queue_<queue_id>_data_<dataset_id>` and `queue_<queue_id>_job_<dataset_id>`.
//!
//! Note: This module intentionally avoids changing behavior; comments and docs were added
//! for clarity on November 10, 2025.
//!
//! Example
//! ```rust,no_run
//! use std::sync::Arc;
//! use event_queue::queue::Queue;
//! use deadpool::Runtime;
//! use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod};
//! use deadpool_postgres::tokio_postgres::NoTls;
//! use serde_json::json;
//! 
//! # async fn example() -> anyhow::Result<()> {
//! // Build a pool (normally from env/config)
//! let mut cfg = Config::new();
//! cfg.dbname = Some("vishal".into());
//! cfg.user = Some("vishal".into());
//! cfg.password = Some("password".into());
//! cfg.host = Some("localhost".into());
//! cfg.port = Some(5432);
//! cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
//! let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
//! 
//! // Obtain a queue by destination id (with default max_duration and max_size)
//! let queue: Arc<Queue> = Queue::get_queue(42, pool.clone(), None, None).await.map_err(|_| anyhow::anyhow!("missing destination"))?;
//! 
//! // Insert some payloads from source id 7
//! queue.insert_data(json!({"event": "signup", "user_id": 1}), 7).await?;
//! queue.insert_data(json!({"event": "click", "path": "/home"}), 7).await?;
//! 
//! // Optionally force a flush (normally automatic by max_size/time)
//! queue.sync_data().await;
//! # Ok(())
//! # }
//! ```

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Batching queue that buffers JSON payloads in-memory and flushes to PostgreSQL.
///
/// Constructed via `Queue::get_queue`, which loads metadata from the `queue` table.
pub struct Queue {
    /// Database identifier of this queue (primary key of `queue` table).
    id: i64,
    /// In-memory buffer mapping UUID -> (JSON payload, source id).
    data: Mutex<HashMap<Uuid, (Value, i64)>>,
    /// Maximum time to wait since the first item was added before auto-flushing.
    max_duration: Duration,
    /// Handle to a scheduled background task that will run a timed sync.
    sync_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Timestamp when the first item was added to an empty buffer; used for timing.
    first_added_at: Mutex<Option<chrono::DateTime<chrono::Utc>>>,
    /// Connection pool used for database operations.
    pool: deadpool_postgres::Pool,
    /// Number of buffered items that triggers an immediate sync when exceeded.
    max_size: i16,
}

impl Queue {
    /// Construct a queue handle for a given `destination_id` using the provided pool.
    ///
    /// Looks up the queue row and initializes the in-memory buffer. Returns an
    /// `Arc<Queue>` so the same queue can be shared across tasks.
    ///
    /// Parameters:
    /// - `destination_id`: The queue destination identifier.
    /// - `pool`: Database connection pool.
    /// - `max_duration`: Maximum time to wait before auto-flushing (default: 10 seconds).
    /// - `max_size`: Maximum number of items before triggering a sync (default: 128).
    ///
    /// Errors:
    /// - Returns `Err(())` if the destination id does not exist.
    pub async fn get_queue(
        destination_id: i64,
        pool: deadpool_postgres::Pool,
        max_duration: Option<Duration>,
        max_size: Option<i16>,
    ) -> Result<Arc<Queue>, ()> {
        let client = pool.get().await.unwrap();
        let stmt = client
            .prepare_cached("SELECT id, destination_id FROM queue WHERE destination_id = $1")
            .await
            .unwrap();
        let mut rows = client.query(&stmt, &[&destination_id]).await.unwrap();

        match rows.pop() {
            None => Err(()),
            Some(row) => Ok(Arc::new(Queue {
                id: row.get(0),
                data: Default::default(),
                max_duration: max_duration.unwrap_or(Duration::from_secs(10)),
                sync_handle: Default::default(),
                first_added_at: Mutex::new(None),
                pool: pool.clone(),
                max_size: max_size.unwrap_or(128),
            })),
        }
    }

    /// Insert a JSON `data` payload attributed to `source_id` into the buffer.
    ///
    /// Behavior:
    /// - If this is the first item in an empty buffer, starts a timed auto-sync.
    /// - If the buffer size exceeds `max_size`, triggers an immediate sync.
    pub async fn insert_data(self: &Arc<Self>, data: Value, source_id: i64) -> Result<(), ()> {
        let uuid = Uuid::new_v4();

        let mut lock = self.data.lock().await;
        lock.insert(uuid, (data, source_id));

        if lock.len() == 1 {
            // Start timer for time-based flush.
            let _ = self.first_added_at.lock().await.insert(chrono::Utc::now());
            self.schedule_auto_sync().await;
        } else if lock.len() > self.max_size as usize {
            // Size-based flush when exceeding max_size.
            self.sync_data().await;
        }

        Ok(())
    }

    /// Cancel any scheduled auto-sync task, waiting for it to finish if running.
    async fn cancel_auto_sync(&self) {
        let mut sync_handle = self.sync_handle.lock().await;
        if let Some(handle) = sync_handle.take() {
            let _ = handle.await;
        }
    }

    /// Schedule an auto-sync to run after `max_duration` if data is still pending.
    ///
    /// Any previously scheduled auto-sync is first canceled to avoid duplicates.
    async fn schedule_auto_sync(self: &Arc<Self>) {
        // Cancel any existing scheduled sync
        self.cancel_auto_sync().await;

        let queue = Arc::clone(self);
        let duration = self.max_duration.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(duration).await;

            // Check if there's still data to sync
            let has_data = {
                let lock = queue.data.lock().await;
                !lock.is_empty()
            };

            if has_data {
                queue.sync_data().await;
            }
        });

        let mut sync_handle = self.sync_handle.lock().await;
        *sync_handle = Some(handle);
    }

    /// Flush buffered data to PostgreSQL using binary `COPY` inside a transaction.
    ///
    /// Steps:
    /// 1. Cancel any pending auto-sync (to avoid duplicate flushes).
    /// 2. Drain the in-memory buffer and clear timing state.
    /// 3. Get a dataset id via `get_current_dataset` to decide the target tables.
    /// 4. COPY buffered rows into `queue_<id>_data_<dataset>` and corresponding job ids
    ///    into `queue_<id>_job_<dataset>`.
    /// 5. Call `release_reservation` with the actual inserted count, then commit.
    pub async fn sync_data(&self) {
        // Cancel any existing scheduled sync
        self.cancel_auto_sync().await;

        let mut client = self.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();

        let data = {
            // Drain the buffer atomically to avoid losing entries on failure later.
            let mut lock = self.data.lock().await;
            let data = lock.drain().collect::<HashMap<_, _>>();

            // Reset timing since the buffer is now empty.
            let _ = self.first_added_at.lock().await.take();

            data
        };

        if data.is_empty() {
            // Nothing to do, but we still commit the empty transaction for cleanliness.
            tx.commit().await.unwrap();
            return;
        }

        // Get the current dataset
        let dataset_id: i32 = tx
            .query_one(
                "SELECT get_current_dataset($1, $2, $3)",
                &[
                    &(self.id as i32),
                    &(self.max_size as i32),
                    &(data.len() as i32),
                ],
            )
            .await
            .unwrap()
            .get(0);

        let data_table_name = format!("queue_{}_data_{}", self.id, dataset_id);
        let job_table_name = format!("queue_{}_job_{}", self.id, dataset_id);

        // Use COPY to bulk insert into data table
        let data_sink = tx
            .copy_in(&format!(
                "COPY {} (id, data, source) FROM STDIN WITH (FORMAT binary)",
                data_table_name
            ))
            .await
            .unwrap();

        let data_writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(
            data_sink,
            &[
                tokio_postgres::types::Type::UUID,
                tokio_postgres::types::Type::JSONB,
                tokio_postgres::types::Type::INT8,
            ],
        );

        futures::pin_mut!(data_writer);

        for (uuid, (json_data, source_id)) in &data {
            // Write one data row: (uuid, jsonb payload, BIGINT source id)
            data_writer
                .as_mut()
                .write(&[uuid, json_data, source_id])
                .await
                .unwrap();
        }

        data_writer.finish().await.unwrap();

        // Use COPY to bulk insert into job table
        let job_sink = tx
            .copy_in(&format!(
                "COPY {} (data) FROM STDIN WITH (FORMAT binary)",
                job_table_name
            ))
            .await
            .unwrap();

        let job_writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(
            job_sink,
            &[tokio_postgres::types::Type::UUID],
        );

        futures::pin_mut!(job_writer);

        for uuid in data.keys() {
            // Write the job row referencing the data UUID.
            job_writer.as_mut().write(&[uuid]).await.unwrap();
        }

        job_writer.finish().await.unwrap();

        // Release the reservation with actual inserted count
        tx.execute(
            "SELECT release_reservation($1, $2, $3)",
            &[&(self.id as i32), &dataset_id, &(data.len() as i32)],
        )
        .await
        .unwrap();

        tx.commit().await.unwrap();
    }
}
