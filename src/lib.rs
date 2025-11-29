//! Event processing library for queue-based asynchronous job execution.
//!
//! This module provides the `EventProcessor` trait and supporting types for implementing
//! custom event processors that consume jobs from PostgreSQL-backed queues. The system
//! handles retry logic, dataset rotation, and concurrent processing with configurable
//! timeouts and limits.
//!
//! # Overview
//!
//! The core abstraction is `EventProcessor`, which implementations must provide:
//! - Queue configuration (ID, concurrency limits, timeouts, retry policies)
//! - Event processing logic via `process()`
//! - Optional hooks for failed event processing, dataset cleanup, and compaction
//!
//! When started via `EventProcessor::start()`, four background tasks are spawned:
//! 1. **Processing**: Continuously fetches and processes pending events
//! 2. **Failed Events**: Handles events that have failed but may be retried
//! 3. **Dataset Cleanup**: Removes old processed datasets
//! 4. **Dataset Compaction**: Compacts failed datasets to reclaim space
//!
//! # Database Schema
//!
//! The system expects a PostgreSQL schema with:
//! - A `queue` table tracking dataset state
//! - Partitioned `queue_<id>_data_<dataset>` tables for event payloads
//! - Partitioned `queue_<id>_job_<dataset>` tables for job metadata and status
//! - A custom PostgreSQL composite type `failed_job_update` for batch updates
//!
//! See `queue.sql` for the complete schema definition.
//!
//! # Example
//!
//! ```rust,no_run
//! use event_queue::EventProcessor;
//! use deadpool_postgres::Pool;
//! use serde_json::Value;
//! use std::time::Duration;
//! use chrono::{DateTime, Utc};
//!
//! struct MyProcessor;
//!
//! #[async_trait::async_trait]
//! impl EventProcessor for MyProcessor {
//!     fn queue_id() -> i32 { 1 }
//!     fn concurrent_processing_limit() -> i64 { 10 }
//!     fn processing_timeout() -> Duration { Duration::from_secs(30) }
//!     fn max_retry_allowed() -> i32 { 3 }
//!     
//!     fn get_retry_at(retry_count: i32) -> DateTime<Utc> {
//!         Utc::now() + Duration::from_secs((retry_count as u64) * 60)
//!     }
//!     
//!     async fn process(event: Value) -> Result<(), event_queue::Error> {
//!         // Custom processing logic
//!         println!("Processing: {:?}", event);
//!         Ok(())
//!     }
//!     
//!     fn delay_for_processing_post_exhaustion() -> Duration {
//!         Duration::from_secs(5)
//!     }
//!     
//!     fn delay_for_dataset_compaction_process() -> Duration {
//!         Duration::from_secs(3600)
//!     }
//!     
//!     fn delay_for_failed_events_processing() -> Duration {
//!         Duration::from_secs(300)
//!     }
//!     
//!     fn delay_for_dataset_cleanup() -> Duration {
//!         Duration::from_secs(3600)
//!     }
//! }
//! ```

use bytes::BufMut;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_postgres::types::{IsNull, ToSql, Type};
use uuid::Uuid;

/// Error type for event processing failures.
///
/// Contains a descriptive message about what went wrong during event processing.
/// Returned by `EventProcessor::process()` when an event cannot be processed successfully.
pub struct Error {
    /// Human-readable error message describing the failure.
    pub message: String,
}

/// Internal representation of a failed job update for PostgreSQL composite type.
///
/// Maps to the PostgreSQL `failed_job_update` composite type defined in the schema.
/// Used for batch updates of failed jobs that should be retried.
#[derive(Debug)]
struct FailedJobUpdate {
    /// Job identifier (primary key in the job table).
    id: i32,
    /// Timestamp when the job should be retried.
    try_at: DateTime<Utc>,
}

/// Helper function to serialize a field value into PostgreSQL binary format.
///
/// Writes the field's OID, length, and binary representation to the output buffer.
/// Used internally by `ToSql` implementation for composite types.
///
/// # Arguments
///
/// * `value` - The value to serialize
/// * `field_type` - PostgreSQL type information
/// * `out` - Output buffer to write binary data
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if serialization fails.
fn write_field<T: ToSql>(
    value: &T,
    field_type: &Type,
    out: &mut bytes::BytesMut,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    // Write the field type OID
    out.put_u32(field_type.oid());

    // Serialize the value
    let mut field_buf = bytes::BytesMut::new();
    match value.to_sql(field_type, &mut field_buf)? {
        IsNull::No => {
            // Write length and data
            out.put_i32(field_buf.len() as i32);
            out.extend_from_slice(&field_buf);
        }
        IsNull::Yes => {
            // Write NULL indicator (-1)
            out.put_i32(-1);
        }
    }
    Ok(())
}

/// Implementation of PostgreSQL binary serialization for `FailedJobUpdate`.
///
/// Encodes the struct as a PostgreSQL composite type matching the `failed_job_update`
/// type defined in the database schema. This allows efficient batch updates of failed
/// jobs using PostgreSQL array operations.
impl ToSql for FailedJobUpdate {
    /// Serialize this struct to PostgreSQL binary format as a composite type.
    ///
    /// The binary format consists of:
    /// 1. Number of fields (i32)
    /// 2. Each field's OID, length, and data
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Write the number of fields in the composite type
        out.put_i32(2);

        // Serialize each field in order
        write_field(&self.id, &Type::INT4, out)?;
        write_field(&self.try_at, &Type::TIMESTAMPTZ, out)?;

        Ok(IsNull::No)
    }

    /// Check if this implementation accepts the given PostgreSQL type.
    ///
    /// Returns `true` only for the `failed_job_update` composite type.
    fn accepts(ty: &Type) -> bool {
        ty.name() == "failed_job_update"
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Handle to all background tasks spawned by an event processor.
///
/// Returned by `EventProcessor::start()`, this struct contains join handles for
/// the four background tasks that continuously process events and maintain the queue.
/// Callers can use these handles to monitor, cancel, or wait for task completion.
pub struct EventProcessHandler {
    /// Handle for the main event processing loop that fetches and processes pending jobs.
    pub processing_handle: JoinHandle<()>,
    /// Handle for the task that processes events that have previously failed.
    pub failed_events_processing_handle: JoinHandle<()>,
    /// Handle for the task that removes old processed datasets to free up space.
    pub dataset_cleanup_handle: JoinHandle<()>,
    /// Handle for the task that compacts failed datasets to optimize storage.
    pub failed_dataset_compaction: JoinHandle<()>,
}

/// Core trait for implementing event processors that consume jobs from PostgreSQL queues.
///
/// Implementations must provide configuration methods (queue ID, concurrency limits, timeouts)
/// and the core `process()` logic for handling individual events. The trait provides a default
/// `start()` implementation that spawns four background tasks for continuous operation.
///
/// # Required Methods
///
/// Implementations must define:
/// - Configuration: `queue_id()`, `concurrent_processing_limit()`, `processing_timeout()`, `max_retry_allowed()`
/// - Processing: `process()` - the core logic to handle a single event
/// - Retry policy: `get_retry_at()` - determines when to retry failed events
/// - Delays: Various delay methods controlling how often background tasks run
///
/// # Optional Methods
///
/// Default implementations are provided for:
/// - `processing_failed_events()` - handle events that have failed but may be retried
/// - `cleanup_processed_datasets()` - remove old datasets to free space
/// - `failed_dataset_compaction()` - compact failed datasets
/// - `process_pending_events()` - main processing loop (rarely needs override)
///
/// # Background Tasks
///
/// When `start()` is called, four infinite-loop tasks are spawned:
/// 1. **Processing Loop**: Fetches pending jobs, processes them concurrently, updates status
/// 2. **Failed Events Processing**: Handles retry logic for failed jobs
/// 3. **Dataset Cleanup**: Removes old processed datasets
/// 4. **Dataset Compaction**: Compacts failed datasets to optimize storage
///
/// Each task sleeps between iterations using the configured delay methods.
#[async_trait::async_trait]
pub trait EventProcessor: Send + Sync {
    /// Start all background processing tasks for this event processor.
    ///
    /// Spawns four independent tokio tasks that run continuously:
    /// - Main processing loop
    /// - Failed events processor
    /// - Dataset cleanup
    /// - Dataset compaction
    ///
    /// # Arguments
    ///
    /// * `pool` - PostgreSQL connection pool shared across all tasks
    ///
    /// # Returns
    ///
    /// Returns `EventProcessHandler` containing join handles for all spawned tasks.
    /// Callers can use these handles to monitor or cancel tasks.
    fn start(pool: Pool) -> EventProcessHandler {
        let pool_processing = pool.clone();
        let processing_handle = tokio::spawn(async move {
            loop {
                Self::process_pending_events(&pool_processing).await;

                tokio::time::sleep(Self::delay_for_processing_post_exhaustion()).await;
            }
        });

        let pool_cleanup = pool.clone();

        let dataset_cleanup_handle = tokio::spawn(async move {
            loop {
                Self::cleanup_processed_datasets(&pool_cleanup).await;

                tokio::time::sleep(Self::delay_for_dataset_cleanup()).await;
            }
        });

        let pool_failed_events = pool.clone();
        let failed_events_processing_handle = tokio::spawn(async move {
            loop {
                Self::processing_failed_events(&pool_failed_events).await;

                tokio::time::sleep(Self::delay_for_failed_events_processing()).await;
            }
        });

        let failed_dataset_compaction = tokio::spawn(async move {
            loop {
                Self::failed_dataset_compaction(&pool).await;

                tokio::time::sleep(Self::delay_for_dataset_compaction_process()).await;
            }
        });

        EventProcessHandler {
            processing_handle,
            failed_events_processing_handle,
            dataset_cleanup_handle,
            failed_dataset_compaction,
        }
    }

    /// Delay to wait after processing all pending events before checking again.
    ///
    /// This prevents tight loops when the queue is empty. Typically set to a few seconds.
    fn delay_for_processing_post_exhaustion() -> Duration;

    /// Delay between dataset compaction runs.
    ///
    /// Controls how often the compaction task runs. Compaction is typically infrequent
    /// (e.g., hourly) as it's a maintenance operation.
    fn delay_for_dataset_compaction_process() -> Duration;

    /// Delay between failed event processing runs.
    ///
    /// Controls how often the system checks for failed events that are ready to retry.
    /// Should be shorter than typical retry delays (e.g., a few minutes).
    fn delay_for_failed_events_processing() -> Duration;

    /// Delay between dataset cleanup runs.
    ///
    /// Controls how often old processed datasets are removed. Typically infrequent
    /// (e.g., hourly or daily) as cleanup is a maintenance operation.
    fn delay_for_dataset_cleanup() -> Duration;

    /// Process a single event.
    ///
    /// This is the core processing logic that implementations must provide.
    /// Called by the main processing loop for each job fetched from the queue.
    ///
    /// # Arguments
    ///
    /// * `event` - JSON payload of the event to process
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if processing succeeds, or `Err(Error)` if it fails.
    /// Failed events will be retried according to the retry policy.
    async fn process(event: Value) -> Result<(), Error>;

    /// The queue identifier in the database.
    ///
    /// This corresponds to the `id` column in the `queue` table and is used
    /// to construct table names like `queue_<id>_data_<dataset>`.
    fn queue_id() -> i32;

    /// Maximum number of jobs to process concurrently.
    ///
    /// Controls batch size when fetching pending jobs. Higher values increase
    /// throughput but also memory usage and database load.
    fn concurrent_processing_limit() -> i64;

    /// Maximum time allowed for processing a single event.
    ///
    /// If a job is not marked as done/failed within this duration, it will be
    /// considered timed out and may be retried. The timeout is updated when
    /// a job transitions to 'processing' status.
    fn processing_timeout() -> Duration;

    /// Maximum number of retry attempts for a failed event.
    ///
    /// After this many failures, the event is marked as 'failed_permanently'
    /// and will not be retried again.
    fn max_retry_allowed() -> i32;

    /// Calculate when a failed event should be retried.
    ///
    /// Called when an event fails to determine the next retry timestamp.
    /// Implementations typically use exponential backoff or other retry strategies.
    ///
    /// # Arguments
    ///
    /// * `retry_count` - Number of times this event has already been retried
    ///
    /// # Returns
    ///
    /// Returns the timestamp when the event should be retried.
    fn get_retry_at(retry_count: i32) -> DateTime<Utc>;

    /// Process events that have previously failed.
    ///
    /// Default implementation is a no-op (todo!()). Implementations can override
    /// to provide custom logic for handling events that have failed but are ready
    /// for retry based on their `try_at` timestamp.
    ///
    /// # Arguments
    ///
    /// * `pool` - Database connection pool
    async fn processing_failed_events(pool: &Pool) {
        todo!()
    }

    /// Clean up old processed datasets to free storage space.
    ///
    /// Default implementation is a no-op (todo!()). Implementations can override
    /// to remove datasets where all jobs are marked as 'done' or 'failed_permanently'.
    ///
    /// # Arguments
    ///
    /// * `pool` - Database connection pool
    async fn cleanup_processed_datasets(pool: &Pool) {
        todo!()
    }

    /// Compact failed datasets to optimize storage.
    ///
    /// Default implementation is a no-op (todo!()). Implementations can override
    /// to compact datasets that contain many failed jobs, potentially archiving
    /// or consolidating them.
    ///
    /// # Arguments
    ///
    /// * `pool` - Database connection pool
    async fn failed_dataset_compaction(pool: &Pool) {
        todo!()
    }

    /// Main processing loop that fetches and processes pending events.
    ///
    /// This is the core of the event processing system. It continuously:
    /// 1. Fetches a batch of pending jobs using row-level locking (FOR UPDATE SKIP LOCKED)
    /// 2. Marks them as 'processing' and updates retry count
    /// 3. Fetches the event data payloads
    /// 4. Processes all events concurrently
    /// 5. Updates job statuses (done/failed/failed_permanently) based on results
    /// 6. Advances to the next dataset when current one is exhausted
    ///
    /// The default implementation handles dataset rotation and retry logic automatically.
    /// Most implementations should not need to override this method.
    ///
    /// # Arguments
    ///
    /// * `pool` - Database connection pool
    async fn process_pending_events(pool: &Pool) {
        let queue_id = Self::queue_id();
        let mut conn = pool.get().await.unwrap();

        // Get current processing and dataset state
        let row = conn
            .query_one(
                "SELECT processing_dataset FROM queue WHERE id = $1",
                &[&queue_id],
            )
            .await
            .unwrap();
        let mut processing_dataset: i32 = row.get(0);

        loop {
            // Construct table names for the current dataset
            let data_table_name = format!("queue_{}_data_{}", &queue_id, processing_dataset);
            let job_table_name = format!("queue_{}_job_{}", &queue_id, processing_dataset);

            loop {
                // Start transaction to atomically fetch and lock jobs
                let transaction = conn.transaction().await.unwrap();

                // Fetch pending jobs with row-level locks (SKIP LOCKED prevents blocking)
                let jobs = transaction
                    .query(
                        &format!("SELECT id, data, try_at FROM {} WHERE status = 'pending' for update skip locked limit $1", job_table_name),
                        &[&Self::concurrent_processing_limit()],
                    )
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| (row.get(0), (row.get(1), row.get(2))))
                    .collect::<HashMap<i32, (Uuid, DateTime<Utc>)>>();

                // Mark jobs as 'processing' and increment retry count
                // Update try_at to current time + timeout for automatic timeout detection
                let retry_data = transaction
                    .query(
                        &format!(
                            "UPDATE {} SET status = 'processing', retry_count = retry_count+1, try_at = $1, updated_at = now() WHERE id = any($2) RETURNING id, retry_count",
                            job_table_name
                        ),
                        &[&(Utc::now() + Self::processing_timeout()), &jobs.keys().collect::<Vec<_>>()],
                    )
                    .await
                    .unwrap().iter().map(|row| (row.get(0), row.get(1))).collect::<HashMap<i32, i32>>();

                transaction.commit().await.unwrap();

                if jobs.is_empty() {
                    break;
                } else {
                    // Extract data UUIDs to fetch event payloads
                    let data_uuids: Vec<Uuid> = jobs.values().map(|(uuid, _)| *uuid).collect();

                    // Fetch event data payloads from data table
                    let events = conn
                        .query(
                            &format!(
                                "SELECT id, data FROM {} WHERE id = any($1)",
                                data_table_name
                            ),
                            &[&data_uuids],
                        )
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| {
                            let uuid: Uuid = row.get(0);
                            let data: Value = row.get(1);
                            (uuid, data)
                        })
                        .collect::<HashMap<Uuid, Value>>();

                    // Process all jobs concurrently
                    let results =
                        futures::future::join_all(jobs.iter().map(|(job_id, (data_uuid, _))| {
                            let event_data = events.get(data_uuid).cloned().unwrap();
                            async move { (*job_id, Self::process(event_data).await) }
                        }))
                        .await;

                    // Categorize results into done, failed, and permanently failed
                    let mut done_jobs: Vec<i32> = Vec::new();
                    let mut permanently_failed_jobs: Vec<i32> = Vec::new();
                    let mut failed_jobs: Vec<FailedJobUpdate> = Vec::new();

                    for (id, res) in results {
                        match res {
                            Ok(_) => done_jobs.push(id),
                            Err(_) => {
                                let retry_count = *retry_data.get(&id).unwrap();

                                if retry_count <= Self::max_retry_allowed() {
                                    // Still within retry limit, schedule for retry
                                    failed_jobs.push(FailedJobUpdate {
                                        id,
                                        try_at: Self::get_retry_at(retry_count),
                                    });
                                } else {
                                    // Exceeded retry limit, mark as permanently failed
                                    permanently_failed_jobs.push(id);
                                }
                            }
                        }
                    }

                    // Batch update all job statuses using the SQL function
                    conn.query(
                        "SELECT update_status($1, $2, $3, $4, $5)",
                        &[
                            &queue_id,
                            &processing_dataset,
                            &done_jobs,
                            &permanently_failed_jobs,
                            &failed_jobs,
                        ],
                    )
                    .await
                    .unwrap();
                }
            }

            // Try to advance to the next dataset
            let mut res = conn.query("UPDATE queue SET processing_dataset=processing_dataset+=1 WHERE id = $1 AND processing_dataset = $2 AND processing_dataset < current_dataset RETURNING processing_dataset", &[&queue_id, &processing_dataset]).await.unwrap();

            let row = if res.is_empty() {
                conn.query_one(
                    "SELECT processing_dataset FROM queue WHERE id = $1",
                    &[&queue_id],
                )
                .await
                .unwrap()
            } else {
                res.pop().unwrap()
            };

            let new_processing_dataset: i32 = row.get(0);

            if processing_dataset == new_processing_dataset {
                break;
            } else {
                processing_dataset = new_processing_dataset;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deadpool::Runtime;
    use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod};
    use tokio_postgres::NoTls;

    struct MockEventProcessor;

    #[async_trait::async_trait]
    impl EventProcessor for MockEventProcessor {
        fn delay_for_processing_post_exhaustion() -> Duration {
            Duration::from_secs(1)
        }
        fn delay_for_dataset_compaction_process() -> Duration {
            Duration::from_secs(1)
        }
        fn delay_for_failed_events_processing() -> Duration {
            Duration::from_secs(1)
        }
        fn delay_for_dataset_cleanup() -> Duration {
            Duration::from_secs(1)
        }

        async fn process(event: Value) -> Result<(), Error> {
            println!("Processing event: {:?}", event);
            Ok(())
        }

        fn queue_id() -> i32 {
            1
        }

        fn concurrent_processing_limit() -> i64 {
            10
        }

        fn processing_timeout() -> Duration {
            Duration::from_secs(3)
        }

        fn max_retry_allowed() -> i32 {
            3
        }

        fn get_retry_at(_: i32) -> DateTime<Utc> {
            Utc::now() + Duration::from_secs(2)
        }
    }

    #[tokio::test]
    async fn test_event_processor() {
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
        let handler = MockEventProcessor::start(pool);

        tokio::time::sleep(Duration::from_secs(1)).await;
        handler.processing_handle.abort();
    }
}
