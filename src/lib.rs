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
    id: Uuid,
    try_at: DateTime<Utc>,
    retry_count: i32,
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
        out.put_i32(3);

        // Write each field
        write_field(&self.id, &Type::UUID, out)?;
        write_field(&self.try_at, &Type::TIMESTAMPTZ, out)?;
        write_field(&self.retry_count, &Type::INT4, out)?;

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "failed_job_update"
    }

    tokio_postgres::types::to_sql_checked!();
}

pub struct EventProcessHandler {
    pub processing_handle: JoinHandle<()>,
}

#[async_trait::async_trait]
pub trait EventProcessor: Send + Sync {
    fn start(pool: Pool) -> EventProcessHandler {
        let pool_clone = pool.clone();
        let processing_handle = tokio::spawn(async move {
            loop {
                Self::process_pending_events(&pool_clone).await;

                tokio::time::sleep(Self::delay_for_processing_post_exhaustion()).await;
            }
        });

        // TODO: Cleanup dataset scheduler
        // TODO: Failed Event Processing
        // TODO: Failed dataset compaction job

        EventProcessHandler { processing_handle }
    }

    fn delay_for_processing_post_exhaustion() -> Duration;

    async fn process(event: Value) -> Result<(), Error>;

    fn queue_id() -> i32;

    fn concurrent_processing_limit() -> i32;

    fn processing_timeout() -> chrono::Duration;

    fn max_retry_allowed() -> i32;

    fn get_retry_at(retry_count: i32) -> DateTime<Utc>;

    async fn process_pending_events(pool: &Pool) {
        let queue_id = Self::queue_id();
        let mut conn = pool.get().await.unwrap();

        loop {
            let row = conn
                .query_one(
                    "SELECT processing_dataset, current_dataset FROM events WHERE queue_id = $1",
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
                    &format!("SELECT data, status, retry_count, try_at FROM {} WHERE status = 'pending' for update skip locked limit $1", job_table_name),
                    &[&Self::concurrent_processing_limit()],
                )
                .await
                .unwrap()
                .iter()
                .map(|row| (row.get(0), (row.get(1), row.get(2), row.get(3))))
                .collect::<HashMap<Uuid, (String, i32, DateTime<chrono::Utc>)>>();

            transaction
                .query(
                    &format!(
                        "UPDATE {} SET status = 'processing', try_at = $1, updated_at = now() WHERE id = any($2)",
                        job_table_name
                    ),
                    &[&(Utc::now() + Self::processing_timeout()), &jobs.keys().collect::<Vec<_>>()],
                )
                .await
                .unwrap();

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
                let ids = jobs.keys().collect::<Vec<_>>();
                let events = conn
                    .query(
                        &format!(
                            "SELECT id, data FROM {} WHERE id = any($1)",
                            data_table_name
                        ),
                        &[&data_table_name, &ids],
                    )
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| (row.get(0), row.get(1)))
                    .collect::<HashMap<Uuid, Value>>();

                let results = futures::future::join_all(
                    events
                        .into_iter()
                        .map(async |(key, v)| (key, Self::process(v).await)),
                )
                .await;

                let mut done_jobs: Vec<Uuid> = Vec::new();
                let mut permanently_failed_jobs: Vec<Uuid> = Vec::new();
                let mut failed_jobs: Vec<FailedJobUpdate> = Vec::new();

                for (id, res) in results {
                    match res {
                        Ok(_) => done_jobs.push(id),
                        Err(_) => {
                            let status = jobs.get(&id).unwrap();

                            if status.1 < Self::max_retry_allowed() {
                                let retry_count = status.1 + 1;
                                failed_jobs.push(FailedJobUpdate {
                                    id,
                                    try_at: Self::get_retry_at(retry_count),
                                    retry_count,
                                });
                            } else {
                                permanently_failed_jobs.push(id);
                            }
                        }
                    }
                }

                conn.query(
                    "PERFORM update_status($1, $2, $3, $4, $5)",
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

        async fn process(event: Value) -> Result<(), Error> {
            println!("Processing event: {:?}", event);
            Ok(())
        }

        fn queue_id() -> i32 {
            1
        }

        fn concurrent_processing_limit() -> i32 {
            10
        }

        fn processing_timeout() -> chrono::Duration {
            chrono::Duration::seconds(5)
        }

        fn max_retry_allowed() -> i32 {
            3
        }

        fn get_retry_at(retry_count: i32) -> DateTime<Utc> {
            Utc::now() + chrono::Duration::seconds(5 * (retry_count as i64))
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

        tokio::time::sleep(Duration::from_secs(30)).await;
        handler.processing_handle.abort();
    }
}
