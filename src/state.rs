use crate::error::InsertionError;
use crate::queue::Queue;
use deadpool_postgres::GenericClient;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub struct State {
    pool: deadpool_postgres::Pool,
    queues: Arc<RwLock<HashMap<i64, Arc<Queue>>>>,
}

impl State {
    pub async fn init(
        pool: deadpool_postgres::Pool,
        max_size: i16,
        max_duration: Duration,
    ) -> Result<Self, deadpool_postgres::PoolError> {
        let client = pool.get().await?;

        let stmt = client
            .prepare_cached("SELECT id, destination_id FROM queue")
            .await?;

        let queues = client
            .query(&stmt, &[])
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.get(1),
                    Arc::new(Queue::get_queue(
                        row.get(0),
                        pool.clone(),
                        Some(max_duration),
                        Some(max_size),
                    )),
                )
            })
            .collect::<HashMap<_, _>>();

        Ok(Self {
            pool,
            queues: Arc::new(RwLock::new(queues)),
        })
    }

    pub async fn add_data(
        &self,
        destination_id: i64,
        data: Vec<Value>,
        source_id: i64,
    ) -> Result<(), InsertionError> {
        let queue = self.queues.read().await.get(&destination_id).cloned();

        match queue {
            Some(queue) => queue.insert_data(data, source_id).await,
            None => {
                let client = self.pool.get().await?;
                let stmt = client
                    .prepare_cached("select id from queue where destination_id = $1")
                    .await?;

                let mut rows = client.query(&stmt, &[&destination_id]).await?;

                if let Some(row) = rows.pop() {
                    let queue =
                        Arc::new(Queue::get_queue(row.get(0), self.pool.clone(), None, None));
                    queue.insert_data(data, source_id).await?;
                    self.queues.write().await.insert(destination_id, queue);
                    return Ok(());
                }

                Err(InsertionError::QueueNotFound)
            }
        }
    }

    pub async fn add_destination(
        &self,
        name: String,
        destination_type: String,
        config: Value,
    ) -> Result<(), InsertionError> {
        let client = self.pool.get().await?;

        let stmt = client
            .prepare_cached("select add_destination($1, $2, $3)")
            .await?;

        let destination_id = client
            .query_one(&stmt, &[&name, &destination_type, &config])
            .await?
            .get(0);

        let stmt = client
            .prepare_cached("select id from queue where destination_id = $1")
            .await?;

        let queue_id = client.query_one(&stmt, &[&destination_id]).await?.get(0);

        let queue = Arc::new(Queue::get_queue(queue_id, self.pool.clone(), None, None));

        self.queues.write().await.insert(destination_id, queue);

        Ok(())
    }
}
