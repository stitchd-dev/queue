//! Binary entrypoint used for quick local connectivity checks.
//! 
//! This example sets up a Deadpool Postgres connection pool and performs a
//! simple `SELECT 1 + $1` query to verify database access. Application code
//! would normally create or obtain a `Queue` from the `queue` module and use
//! it to buffer and flush events.

pub mod queue;

use deadpool::Runtime;
use deadpool_postgres::tokio_postgres::NoTls;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod};

#[tokio::main]
async fn main() {
    // Configure the Postgres pool. In production, prefer reading these from env vars.
    let mut config = Config::new();
    config.dbname = Some("vishal".to_string());
    config.user = Some("vishal".to_string());
    config.password = Some("password".to_string());
    config.host = Some("localhost".to_string());
    config.port = Some(5432);

    // Use fast recycling which avoids full connection reset where safe.
    config.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    let pool = config.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

    // Quick smoke test against the database.
    println!("{:?}", pool);

    for i in 1..10i32 {
        let client = pool.get().await.unwrap();
        let stmt = client.prepare_cached("SELECT 1 + $1").await.unwrap();
        let rows = client.query(&stmt, &[&i]).await.unwrap();
        let value: i32 = rows[0].get(0);
        assert_eq!(value, i + 1);
    }
}
