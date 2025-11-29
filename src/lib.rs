use bytes::BufMut;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_postgres::types::{IsNull, ToSql, Type};
use uuid::Uuid;

pub struct Error {
    pub message: String,
}

#[derive(Debug)]
struct FailedJobUpdate {
    id: i32,
    try_at: DateTime<Utc>,
}

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

impl ToSql for FailedJobUpdate {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Convert the struct to a tuple format that matches the PostgreSQL composite type

        // Write the number of fields
        out.put_i32(2);

        // Write each field
        write_field(&self.id, &Type::INT4, out)?;
        write_field(&self.try_at, &Type::TIMESTAMPTZ, out)?;

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "failed_job_update"
    }

    tokio_postgres::types::to_sql_checked!();
}

pub struct EventProcessHandler {
    pub processing_handle: JoinHandle<()>,
    pub failed_events_processing_handle: JoinHandle<()>,
    pub dataset_cleanup_handle: JoinHandle<()>,
    pub failed_dataset_compaction: JoinHandle<()>,
}

#[async_trait::async_trait]
pub trait EventProcessor: Send + Sync {
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

    fn delay_for_processing_post_exhaustion() -> Duration;
    fn delay_for_dataset_compaction_process() -> Duration;
    fn delay_for_failed_events_processing() -> Duration;
    fn delay_for_dataset_cleanup() -> Duration;

    async fn process(event: Value) -> Result<(), Error>;

    fn queue_id() -> i32;

    fn concurrent_processing_limit() -> i64;

    fn processing_timeout() -> Duration;

    fn max_retry_allowed() -> i32;

    fn get_retry_at(retry_count: i32) -> DateTime<Utc>;

    async fn processing_failed_events(pool: &Pool) {
        todo!()
    }

    async fn cleanup_processed_datasets(pool: &Pool) {
        todo!()
    }

    async fn failed_dataset_compaction(pool: &Pool) {
        todo!()
    }

    async fn process_pending_events(pool: &Pool) {
        let queue_id = Self::queue_id();
        let mut conn = pool.get().await.unwrap();

        loop {
            let row = conn
                .query_one(
                    "SELECT processing_dataset, current_dataset FROM queue WHERE id = $1",
                    &[&queue_id],
                )
                .await
                .unwrap();
            let (processing_dataset, current_dataset): (i32, i32) = (row.get(0), row.get(1));

            let data_table_name = format!("queue_{}_data_{}", &queue_id, current_dataset);
            let job_table_name = format!("queue_{}_job_{}", &queue_id, current_dataset);

            let transaction = conn.transaction().await.unwrap();
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
                if processing_dataset == current_dataset {
                    break;
                } else {
                    let res = conn.query("UPDATE queue SET processing_dataset=processing_dataset+=1 WHERE id = $1 AND processing_dataset = $2 RETURNING id", &[&queue_id, &processing_dataset]).await.unwrap();

                    if res.is_empty() {
                        break;
                    }
                }
            } else {
                let data_uuids: Vec<Uuid> = jobs.values().map(|(uuid, _)| *uuid).collect();

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

                let results =
                    futures::future::join_all(jobs.iter().map(|(job_id, (data_uuid, _))| {
                        let event_data = events.get(data_uuid).cloned().unwrap();
                        async move { (*job_id, Self::process(event_data).await) }
                    }))
                    .await;

                let mut done_jobs: Vec<i32> = Vec::new();
                let mut permanently_failed_jobs: Vec<i32> = Vec::new();
                let mut failed_jobs: Vec<FailedJobUpdate> = Vec::new();

                for (id, res) in results {
                    match res {
                        Ok(_) => done_jobs.push(id),
                        Err(_) => {
                            let retry_count = *retry_data.get(&id).unwrap();

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

                conn.query(
                    "SELECT update_status($1, $2, $3, $4, $5)",
                    &[
                        &queue_id,
                        &current_dataset,
                        &done_jobs,
                        &permanently_failed_jobs,
                        &failed_jobs,
                    ],
                )
                .await
                .unwrap();
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
