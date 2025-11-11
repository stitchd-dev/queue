# Stitchd - Event / Message Queue

A minimal Rust-based event buffering queue that batches JSON payloads in memory and flushes them to PostgreSQL using
efficient binary COPY. Flushing is triggered either when the buffer grows beyond a threshold or when a maximum wait
duration elapses since the first item was added.

## Features

- In-memory buffering with backpressure threshold
- Time-based and size-based flush triggers
- Single-transaction bulk flush using PostgreSQL binary `COPY`
- Simple concurrency model using `tokio::sync::Mutex`
- Deadpool Postgres connection pooling

## How it works

The core logic lives in `src/queue.rs`:

- `Queue::insert_data` adds payloads to the buffer and schedules a timed auto-sync if it was empty. Exceeding the
  `threshold` triggers an immediate flush.
- `Queue::sync_data` drains the buffer and copies rows into per-dataset tables:
    - Data table: `queue_<queue_id>_data_<dataset_id>` with columns `(id uuid, data jsonb, source int)`
    - Job table: `queue_<queue_id>_job_<dataset_id>` with column `(data uuid, ...)`

A helper function `get_current_dataset(queue_id, threshold, incoming_count)` selects the active dataset id. After
inserting, `release_reservation(queue_id, dataset_id, count)` finalizes the reservation.

See rich module and method documentation in `src/queue.rs`.

## Database schema

The SQL script `queue.sql` sets up a reference schema and the helper functions used by the Rust code. It also includes
maintenance helpers for creating dataset tables and managing job status.

Apply it to your Postgres instance:

```sh
psql postgresql://USER:PASSWORD@localhost:5432/DBNAME -f queue.sql
```

Replace `USER`, `PASSWORD`, and `DBNAME` with your values.

## Quick start (dev)

1. Ensure Postgres is running and apply `queue.sql` as shown above.
2. Adjust the demo connection settings in `src/main.rs` to match your environment.
3. Run the binary:

```sh
cargo run
```

This performs a simple connectivity check (`SELECT 1 + $1`) to verify the pool and DB access.

## Using the Queue in your code

```rust
use std::sync::Arc;
use event_queue::queue::Queue;
use deadpool::Runtime;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod};
use deadpool_postgres::tokio_postgres::NoTls;
use serde_json::json;

#[tokio::main]
async fn main() {
    let mut cfg = Config::new();
    cfg.dbname = Some("vishal".into());
    cfg.user = Some("vishal".into());
    cfg.password = Some("password".into());
    cfg.host = Some("localhost".into());
    cfg.port = Some(5432);
    cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

    let q: Arc<Queue> = Queue::get_queue(42, pool.clone(), None, None)
        .await
        .expect("destination must exist");

    q.insert_data(json!({"event": "signup", "user_id": 1}), 7).await.unwrap();
    q.insert_data(json!({"event": "click", "path": "/home"}), 7).await.unwrap();

    // Optional: force a flush (normally automatic)
    q.sync_data().await;
}
```

## Configuration notes

- For production, prefer reading DB configuration from environment variables or a config file.
- Tune `threshold` and `max_duration` in `Queue` to balance latency vs. efficiency.

## License

MIT (or your choice).