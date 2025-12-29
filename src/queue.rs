//! Queue module
//!
//! This module implements an in-memory buffering queue that batches JSON payloads
//! and periodically flushes them into PostgreSQL using the efficient binary ` COPY `.
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
//! // Build a connection_pool (normally from env/config)
//! let mut cfg = Config::new();
//! cfg.dbname = Some("vishal".into());
//! cfg.user = Some("vishal".into());
//! cfg.password = Some("password".into());
//! cfg.host = Some("localhost".into());
//! cfg.port = Some(5432);
//! cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
//! let connection_pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
//!
//! // Obtain a queue by destination id (with default max_duration and max_size)
//! let queue: Arc<Queue> = Queue::get_queue(42, connection_pool.clone(), None, None).await.map_err(|_| anyhow::anyhow!("missing destination"))?;
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

use crate::error::{InsertionError, SyncError};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Sleep;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};
use tracing::{debug, error};
use uuid::Uuid;

/// Batching queue that buffers JSON payloads in-memory and flushes to PostgreSQL.
///
/// Constructed via `Queue::get_queue`, which loads metadata from the `queue` table.
#[derive(Clone)]
pub struct Queue {
    /// Database identifier of this queue (primary key of `queue` table).
    id: i32,
    /// Maximum time to wait since the first item was added before auto-flushing.
    max_duration: Duration,
    /// Connection connection_pool used for database operations.
    pool: deadpool_postgres::Pool,
    /// Number of buffered items that triggers an immediate sync when exceeded.
    max_buffer_size: usize,
    /// Max Events Allowed per dataset
    max_events_per_dataset: i32,
    sender: mpsc::Sender<Vec<Value>>,
}

impl Queue {
    /// Construct a queue handle for a given `destination_id` using the provided connection_pool.
    ///
    /// Looks up the queue row and initializes the in-memory buffer. Returns an
    /// `Arc<Queue>` so the same queue can be shared across tasks.
    ///
    /// Parameters:
    /// - `destination_id`: The queue destination identifier.
    /// - `connection_pool`: Database connection connection_pool.
    /// - `max_duration`: Maximum time to wait before auto-flushing (default: 10 seconds).
    /// - `max_size`: Maximum number of items before triggering a sync (default: 128).
    ///
    /// Errors:
    /// - Returns `Err(())` if the destination id does not exist.
    pub fn get_queue(
        id: i32,
        pool: deadpool_postgres::Pool,
        max_duration: Option<Duration>,
        max_buffer_size: Option<u8>,
        max_events_per_dataset: Option<u32>,
    ) -> Self {
        let max_buffer_size = match max_buffer_size {
            None => u8::MAX / 2,
            Some(v) => {
                if v == 0 {
                    u8::MAX / 2
                } else {
                    v
                }
            }
        } as usize;

        let max_events_per_dataset = match max_events_per_dataset {
            Some(d) => {
                if d == 0 {
                    i32::MAX
                } else if d > i32::MAX as u32 {
                    error!("Limiting to i32::MAX");

                    i32::MAX
                } else {
                    d as i32
                }
            }
            None => i32::MAX,
        };

        let (sender, mut receiver) = mpsc::channel::<Vec<Value>>(20);
        let max_duration = max_duration.unwrap_or(Duration::from_secs(10));

        let pool_clone = pool.clone();
        tokio::spawn(async move {
            let mut buffer: Vec<Value> = Vec::with_capacity(5 * max_buffer_size);

            let mut buf: Vec<Vec<Value>> = Vec::with_capacity(4);
            while receiver.recv_many(&mut buf, 4).await > 0 {
                buffer.extend(buf.drain(..).flatten());
                if buffer.len() >= max_buffer_size {
                    let _ = send_data_to_pg(
                        &pool_clone,
                        id,
                        max_events_per_dataset,
                        buffer.drain(..).collect(),
                    )
                    .await;
                }
            }

            if buffer.len() > 0 {
                let _ = send_data_to_pg(
                    &pool_clone,
                    id,
                    max_events_per_dataset,
                    buffer.drain(..).collect(),
                )
                .await;
            }
        });

        Queue {
            id,
            max_duration,
            pool: pool.clone(),
            max_buffer_size,
            max_events_per_dataset,
            sender,
        }
    }

    /// Insert a JSON `data` payload attributed to `source_id` into the buffer.
    ///
    /// Behavior:
    /// - If this is the first item in an empty buffer, starts a timed auto-sync.
    /// - If the buffer size exceeds `max_size`, triggers an immediate sync.
    pub async fn insert_data(self: &Arc<Self>, data: Vec<Value>) -> Result<(), InsertionError> {
        let len = data.len();

        if len > self.max_buffer_size {
            return Err(InsertionError::LimitExceeded(self.max_buffer_size));
        } else if len == 0 {
            return Err(InsertionError::EmptyData);
        } else {
            self.sender.send(data).await?;
        }

        Ok(())
    }
}

/// Internal helper to send data to PostgreSQL.
///
/// This method performs the actual database operations to persist buffered data:
/// 1. Acquires a database connection and starts a transaction.
/// 2. Calls `get_current_dataset` to determine the target dataset.
/// 3. Uses binary COPY to bulk insert data into the data and job tables.
/// 4. Calls `release_reservation` to finalize the dataset reservation.
/// 5. Commits the transaction.
async fn send_data_to_pg(
    pool: &deadpool_postgres::Pool,
    queue_id: i32,
    max_events_per_dataset: i32,
    data: Vec<Value>,
) -> Result<(), SyncError> {
    debug!("Sending data to PG.");
    let mut client = pool.get().await?;

    let tx = client.transaction().await?;

    // Get the current dataset
    let dataset_id: i32 = tx
        .query_one(
            "SELECT get_current_dataset($1, $2, $3)",
            &[&(queue_id), &(data.len() as i32), &(max_events_per_dataset)],
        )
        .await
        .map_err(|err| {
            println!("{}", err);
            err
        })?
        .get(0);

    let data_table_name = format!("queue_{}_data_{}", queue_id, dataset_id);
    let job_table_name = format!("queue_{}_job_{}", queue_id, dataset_id);

    // Use COPY to bulk insert into data table
    let data_sink = tx
        .copy_in(&format!(
            "COPY {} (id, data) FROM STDIN WITH (FORMAT binary)",
            data_table_name
        ))
        .await?;

    let data_writer = BinaryCopyInWriter::new(data_sink, &[Type::UUID, Type::JSONB]);

    futures::pin_mut!(data_writer);

    let data = data
        .into_iter()
        .map(|v| (Uuid::new_v4(), v))
        .collect::<HashMap<_, _>>();

    for (uuid, json_data) in &data {
        // Write one data row: (uuid, jsonb payload, BIGINT source id)
        data_writer.as_mut().write(&[uuid, json_data]).await?;
    }

    data_writer.finish().await?;

    // Use COPY to bulk insert into job table
    let job_sink = tx
        .copy_in(&format!(
            "COPY {} (data) FROM STDIN WITH (FORMAT binary)",
            job_table_name
        ))
        .await?;

    let job_writer = BinaryCopyInWriter::new(job_sink, &[Type::UUID]);

    futures::pin_mut!(job_writer);

    for uuid in data.keys() {
        // Write the job row referencing the data UUID.
        job_writer.as_mut().write(&[uuid]).await?;
    }

    job_writer.finish().await?;

    // Release the reservation with an actual inserted count
    tx.execute(
        "SELECT release_reservation($1, $2, $3)",
        &[&queue_id, &dataset_id, &(data.len() as i32)],
    )
    .await?;

    tx.commit().await?;

    Ok(())
}
