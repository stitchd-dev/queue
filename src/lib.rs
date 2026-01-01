//! Event processing library for PostgreSQL-backed queue-based asynchronous job execution.

use bytes::BufMut;
use chrono::{DateTime, Utc};
use deadpool_postgres::{Object, Pool};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_postgres::types::{IsNull, ToSql, Type};
use tracing::debug;
use uuid::Uuid;

/// Error type for event processing failures.
pub struct Error {
    /// Error message.
    pub message: String,
}

/// Failed job update for PostgreSQL composite type.
#[derive(Debug)]
struct FailedJobUpdate {
    /// Job identifier.
    id: i32,
    /// Retry timestamp.
    try_at: DateTime<Utc>,
}

/// Serializes a field value into PostgreSQL binary format.
fn write_field<T: ToSql>(
    value: &T,
    field_type: &Type,
    out: &mut bytes::BytesMut,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    out.put_u32(field_type.oid());

    let mut field_buf = bytes::BytesMut::new();
    match value.to_sql(field_type, &mut field_buf)? {
        IsNull::No => {
            out.put_i32(field_buf.len() as i32);
            out.extend_from_slice(&field_buf);
        }
        IsNull::Yes => {
            out.put_i32(-1);
        }
    }
    Ok(())
}

/// PostgreSQL binary serialization for `FailedJobUpdate`.
impl ToSql for FailedJobUpdate {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_i32(2);
        write_field(&self.id, &Type::INT4, out)?;
        write_field(&self.try_at, &Type::TIMESTAMPTZ, out)?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "failed_job_update"
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Handle to background tasks spawned by an event processor.
pub struct EventProcessHandler {
    /// Main event processing handle.
    pub processing_handle: JoinHandle<()>,
    /// Failed events processing handle.
    pub failed_events_processing_handle: JoinHandle<()>,
    /// Dataset cleanup handle.
    pub dataset_cleanup_handle: JoinHandle<()>,
    /// Failed dataset compaction handle.
    pub failed_dataset_compaction: JoinHandle<()>,
}

/// Internal error type for tracking processing failures with retry information.
struct ProcessError {
    /// Number of times this job has been retried.
    retry_count: i32,
    /// The error that occurred during processing.
    _error: Error,
}

/// Represents a job to be processed from the queue.
pub struct Job {
    /// UUID of the event data in the data table.
    data_uuid: Uuid,
    /// Number of times this job has been retried.
    retry_count: i32,
}

/// Trait for implementing event processors that consume jobs from PostgreSQL queues.
#[async_trait::async_trait]
pub trait EventProcessor: Send + Sync {
    /// Starts all background processing tasks.
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

    /// Delay after processing all pending events before checking again.
    fn delay_for_processing_post_exhaustion() -> Duration;

    /// Delay between dataset compaction runs.
    fn delay_for_dataset_compaction_process() -> Duration;

    /// Delay between failed event processing runs.
    fn delay_for_failed_events_processing() -> Duration;

    /// Delay between dataset cleanup runs.
    fn delay_for_dataset_cleanup() -> Duration;

    /// Processes a single event from the queue.
    async fn process(event: Value) -> Result<(), Error>;

    /// Queue identifier in the database.
    fn queue_id() -> i32;

    /// Maximum number of jobs to process concurrently.
    fn concurrent_processing_limit() -> i64;

    /// Maximum time allowed for processing a single event.
    fn processing_timeout() -> Duration;

    /// Maximum number of retry attempts for a failed event.
    fn max_retry_allowed() -> i32;

    /// Calculates when a failed event should be retried.
    fn get_retry_at(retry_count: i32) -> DateTime<Utc>;

    /// Processes events that have previously failed.
    async fn processing_failed_events(pool: &Pool) {
        let queue_id = Self::queue_id();
        let mut conn = pool.get().await.unwrap();

        let mut last_failed_dataset: i32 = get_last_failed_dataset(&conn, &queue_id).await;

        loop {
            debug!(
                "Processing failed events for dataset {}",
                last_failed_dataset
            );
            let data_table_name = format!("queue_{}_data_{}", &queue_id, last_failed_dataset);
            let job_table_name = format!("queue_{}_job_{}", &queue_id, last_failed_dataset);

            loop {
                debug!("Processing Jobs");

                let jobs = get_failed_jobs(
                    &mut conn,
                    &job_table_name,
                    &Self::concurrent_processing_limit(),
                    Self::processing_timeout(),
                )
                .await;

                if jobs.is_empty() {
                    break;
                } else {
                    Self::_process_jobs(
                        &queue_id,
                        &mut conn,
                        last_failed_dataset,
                        &data_table_name,
                        &jobs,
                    )
                    .await;
                }
            }

            debug!("Advancing Failed Dataset");

            match get_next_failed_dataset(
                &queue_id,
                &mut conn,
                last_failed_dataset,
                &job_table_name,
            )
            .await
            {
                Some(new_dataset) => last_failed_dataset = new_dataset,
                None => break,
            }
        }
    }

    /// Cleans up old processed datasets.
    async fn cleanup_processed_datasets(pool: &Pool) {
        let conn = pool.get().await.unwrap();

        let queue_id = Self::queue_id();

        conn.query("SELECT cleanup_dataset($1)", &[&queue_id])
            .await
            .unwrap();
    }

    /// Compacts failed datasets to optimize storage.
    async fn failed_dataset_compaction(_pool: &Pool) {
        // TODO
    }

    /// Main processing loop that fetches and processes pending events.
    async fn process_pending_events(pool: &Pool) {
        let queue_id = Self::queue_id();
        let mut conn = pool.get().await.unwrap();

        let mut processing_dataset: i32 = get_processing_dataset(&conn, &queue_id).await;

        loop {
            debug!("Processing dataset {}", processing_dataset);
            let data_table_name = format!("queue_{}_data_{}", &queue_id, processing_dataset);
            let job_table_name = format!("queue_{}_job_{}", &queue_id, processing_dataset);

            loop {
                debug!("Processing Jobs");

                let jobs = get_pending_jobs(
                    &mut conn,
                    &job_table_name,
                    &Self::concurrent_processing_limit(),
                    Self::processing_timeout(),
                )
                .await;

                if jobs.is_empty() {
                    break;
                } else {
                    Self::_process_jobs(
                        &queue_id,
                        &mut conn,
                        processing_dataset,
                        &data_table_name,
                        &jobs,
                    )
                    .await;
                }
            }

            // Try to advance to the next dataset
            debug!("Advancing dataset");

            match get_next_processing_dataset(&queue_id, &mut conn, processing_dataset).await {
                Some(new_dataset) => processing_dataset = new_dataset,
                None => break,
            }
        }
    }

    /// Internal method to process a batch of jobs.
    ///
    /// Fetches event data, processes each job concurrently, and updates job statuses
    /// based on success or failure.
    async fn _process_jobs(
        queue_id: &i32,
        conn: &mut Object,
        processing_dataset: i32,
        data_table_name: &String,
        jobs: &HashMap<i32, Job>,
    ) {
        let events = get_events(conn, data_table_name, jobs).await;

        let results = futures::future::join_all(jobs.iter().map(|(job_id, job)| {
            let event_data = events.get(&job.data_uuid).cloned().unwrap();
            async move {
                (
                    *job_id,
                    Self::process(event_data)
                        .await
                        .map_err(|error| ProcessError {
                            retry_count: job.retry_count,
                            _error: error,
                        }),
                )
            }
        }))
        .await;

        let mut done_jobs: Vec<i32> = Vec::new();
        let mut permanently_failed_jobs: Vec<i32> = Vec::new();
        let mut failed_jobs: Vec<FailedJobUpdate> = Vec::new();

        for (id, res) in results {
            match res {
                Ok(_) => done_jobs.push(id),
                Err(err) => {
                    let retry_count = err.retry_count;

                    if retry_count <= Self::max_retry_allowed() {
                        failed_jobs.push(FailedJobUpdate {
                            id,
                            try_at: Self::get_retry_at(retry_count),
                        });
                    } else {
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

/// Retrieves the current processing dataset ID for a queue.
///
/// # Arguments
/// * `conn` - Database connection.
/// * `queue_id` - The queue identifier.
///
/// # Returns
/// The current processing dataset ID.
async fn get_processing_dataset(conn: &Object, queue_id: &i32) -> i32 {
    let row = conn
        .query_one(
            "SELECT processing_dataset FROM queue WHERE id = $1",
            &[queue_id],
        )
        .await
        .unwrap();
    row.get(0)
}

/// Retrieves the last failed dataset ID for a queue.
///
/// # Arguments
/// * `conn` - Database connection.
/// * `queue_id` - The queue identifier.
///
/// # Returns
/// The last failed dataset ID.
async fn get_last_failed_dataset(conn: &Object, queue_id: &i32) -> i32 {
    let row = conn
        .query_one(
            "SELECT last_failed_dataset FROM queue WHERE id = $1",
            &[queue_id],
        )
        .await
        .unwrap();
    row.get(0)
}

/// Attempts to advance to the next processing dataset.
///
/// # Arguments
/// * `queue_id` - The queue identifier.
/// * `conn` - Database connection.
/// * `processing_dataset` - Current processing dataset ID.
///
/// # Returns
/// `Some(new_dataset_id)` if advanced, `None` if no more datasets to process.
async fn get_next_processing_dataset(
    queue_id: &i32,
    conn: &mut Object,
    processing_dataset: i32,
) -> Option<i32> {
    let mut res = conn.query("UPDATE queue SET processing_dataset=processing_dataset+1 WHERE id = $1 AND processing_dataset = $2 AND processing_dataset < current_dataset RETURNING processing_dataset", &[&queue_id, &processing_dataset]).await.unwrap();

    let new_processing_dataset: i32 = if res.is_empty() {
        get_processing_dataset(conn, queue_id).await
    } else {
        res.pop().unwrap().get(0)
    };

    match new_processing_dataset == processing_dataset {
        true => None,
        false => Some(new_processing_dataset),
    }
}

/// Attempts to advance to the next failed dataset for retry processing.
///
/// # Arguments
/// * `queue_id` - The queue identifier.
/// * `conn` - Database connection.
/// * `last_failed_dataset` - Current failed dataset ID.
/// * `job_table_name` - Name of the job table to check.
///
/// # Returns
/// `Some(new_dataset_id)` if advanced, `None` if no more failed datasets or current has pending jobs.
async fn get_next_failed_dataset(
    queue_id: &i32,
    conn: &mut Object,
    last_failed_dataset: i32,
    job_table_name: &str,
) -> Option<i32> {
    let mut res = conn
        .query(
            &format!(
                "SELECT EXISTS (
                            SELECT 1 FROM {}
                            WHERE status IN ('processing', 'failed')
                            )",
                job_table_name
            ),
            &[],
        )
        .await
        .unwrap();

    if res.pop()?.get(0) {
        None
    } else {
        let mut res = conn.query("UPDATE queue SET last_failed_dataset=last_failed_dataset+1 WHERE id = $1 AND last_failed_dataset = $2 AND last_failed_dataset < processing_dataset RETURNING last_failed_dataset", &[&queue_id, &last_failed_dataset]).await.unwrap();

        let new_last_failed_dataset: i32 = if res.is_empty() {
            get_last_failed_dataset(conn, queue_id).await
        } else {
            res.pop().unwrap().get(0)
        };

        match new_last_failed_dataset == last_failed_dataset {
            true => None,
            false => Some(new_last_failed_dataset),
        }
    }
}

/// Fetches event data for a batch of jobs.
///
/// # Arguments
/// * `conn` - Database connection.
/// * `data_table_name` - Name of the data table.
/// * `jobs` - Map of job IDs to Job structs.
///
/// # Returns
/// HashMap mapping data UUIDs to their JSON values.
async fn get_events(
    conn: &mut Object,
    data_table_name: &String,
    jobs: &HashMap<i32, Job>,
) -> HashMap<Uuid, Value> {
    let data_uuids: Vec<Uuid> = jobs.values().map(|job| job.data_uuid).collect();

    conn.query(
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
        let data: Vec<u8> = row.get(1);
        (uuid, serde_json::from_slice(&data).unwrap())
    })
    .collect::<HashMap<Uuid, Value>>()
}

/// Fetches and locks failed or timed-out jobs for retry processing.
///
/// # Arguments
/// * `conn` - Database connection.
/// * `job_table_name` - Name of the job table.
/// * `concurrent_processing_limit` - Maximum number of jobs to fetch.
/// * `processing_timeout` - Duration to set for next retry attempt.
///
/// # Returns
/// HashMap of job IDs to Job structs ready for processing.
async fn get_failed_jobs(
    conn: &mut Object,
    job_table_name: &String,
    concurrent_processing_limit: &i64,
    processing_timeout: Duration,
) -> HashMap<i32, Job> {
    conn.query(
        &format!(
            "UPDATE {} SET
                    status = 'processing',
                    retry_count = retry_count + 1,
                    try_at = $1,
                    updated_at = now()
                 WHERE id IN (
                    SELECT id FROM {}
                    WHERE (status = 'failed' OR status = 'processing') AND try_at < now()
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT $2
                 )
                 RETURNING id, data, retry_count",
            job_table_name, job_table_name
        ),
        &[
            &(Utc::now() + processing_timeout),
            concurrent_processing_limit,
        ],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| {
        (
            row.get(0),
            Job {
                data_uuid: row.get(1),
                retry_count: row.get(2),
            },
        )
    })
    .collect::<HashMap<i32, Job>>()
}

/// Fetches and locks pending jobs for initial processing.
///
/// # Arguments
/// * `conn` - Database connection.
/// * `job_table_name` - Name of the job table.
/// * `concurrent_processing_limit` - Maximum number of jobs to fetch.
/// * `processing_timeout` - Duration to set for processing timeout.
///
/// # Returns
/// HashMap of job IDs to Job structs ready for processing.
async fn get_pending_jobs(
    conn: &mut Object,
    job_table_name: &String,
    concurrent_processing_limit: &i64,
    processing_timeout: Duration,
) -> HashMap<i32, Job> {
    conn.query(
        &format!(
            "UPDATE {} SET 
                    status = 'processing', 
                    retry_count = retry_count + 1, 
                    try_at = $1, 
                    updated_at = now() 
                 WHERE id IN (
                    SELECT id FROM {} 
                    WHERE status = 'pending' 
                    ORDER BY id 
                    FOR UPDATE SKIP LOCKED 
                    LIMIT $2
                 ) 
                 RETURNING id, data, retry_count",
            job_table_name, job_table_name
        ),
        &[
            &(Utc::now() + processing_timeout),
            concurrent_processing_limit,
        ],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| {
        (
            row.get(0),
            Job {
                data_uuid: row.get(1),
                retry_count: row.get(2),
            },
        )
    })
    .collect::<HashMap<i32, Job>>()
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
            Duration::from_secs(100000)
        }
        fn delay_for_failed_events_processing() -> Duration {
            Duration::from_secs(5)
        }
        fn delay_for_dataset_cleanup() -> Duration {
            Duration::from_secs(20)
        }

        async fn process(event: Value) -> Result<(), Error> {
            debug!("Processing event: {:?}", event);
            Ok(())
        }

        fn queue_id() -> i32 {
            2
        }

        fn concurrent_processing_limit() -> i64 {
            256
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

        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}
