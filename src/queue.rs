use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Queue {
    id: i64,
    data: Mutex<HashMap<Uuid, (Value, i64)>>,
    max_duration: Duration,
    sync_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    first_added_at: Mutex<Option<chrono::DateTime<chrono::Utc>>>,
    pool: deadpool_postgres::Pool,
    threshold: i16,
}

impl Queue {
    pub async fn get_queue(
        destination_id: i64,
        pool: deadpool_postgres::Pool,
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
                max_duration: Duration::from_secs(10),
                sync_handle: Default::default(),
                first_added_at: Mutex::new(None),
                pool: pool.clone(),
                threshold: 128,
            })),
        }
    }

    pub async fn insert_data(self: &Arc<Self>, data: Value, source_id: i64) -> Result<(), ()> {
        let uuid = Uuid::new_v4();

        let mut lock = self.data.lock().await;
        lock.insert(uuid, (data, source_id));

        if lock.len() == 1 {
            let _ = self.first_added_at.lock().await.insert(chrono::Utc::now());
            self.schedule_auto_sync().await;
        } else if lock.len() > self.threshold as usize {
            self.sync_data().await;
        }

        Ok(())
    }

    async fn cancel_auto_sync(&self) {
        let mut sync_handle = self.sync_handle.lock().await;
        if let Some(handle) = sync_handle.take() {
            let _ = handle.await;
        }
    }

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

    pub async fn sync_data(&self) {
        // Cancel any existing scheduled sync
        self.cancel_auto_sync().await;

        let mut client = self.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();

        let data = {
            let mut lock = self.data.lock().await;
            let data = lock.drain().collect::<HashMap<_, _>>();

            let _ = self.first_added_at.lock().await.take();

            data
        };

        if data.is_empty() {
            tx.commit().await.unwrap();
            return;
        }

        // Get the current dataset
        let dataset_id: i32 = tx
            .query_one(
                "SELECT get_current_dataset($1, $2, $3)",
                &[
                    &(self.id as i32),
                    &(self.threshold as i32),
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
