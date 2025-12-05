use crate::connection_pool::{get_connection_pool_health, get_process_pool_health};
use deadpool::Status;

pub struct PoolState {
    pub(crate) active: usize,
    pub(crate) total: usize,
}

pub struct Health {
    connections: PoolState,
    processors: PoolState,
    db_pool: Status,
}

impl Health {
    pub fn get_health(db_pool: deadpool_postgres::Pool) -> Health {
        let connections = get_connection_pool_health();
        let processors = get_process_pool_health();

        Health {
            connections,
            processors,
            db_pool: db_pool.status(),
        }
    }
}
