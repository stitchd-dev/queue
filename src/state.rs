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
    ) -> Self {
        let client = pool.get().await.unwrap();

        let stmt = client
            .prepare_cached("SELECT id, destination_id FROM queue")
            .await
            .unwrap();

        let queues = client
            .query(&stmt, &[])
            .await
            .unwrap()
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

        Self {
            pool,
            queues: Arc::new(RwLock::new(queues)),
        }
    }

    pub async fn add_data(&self, destination_id: i64, data: Vec<Value>, source_id: i64) {
        let queue = self
            .queues
            .read()
            .await
            .get(&destination_id)
            .unwrap()
            .clone();

        queue.insert_data(data, source_id).await.unwrap();
    }
}
