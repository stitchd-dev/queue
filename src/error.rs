use deadpool_postgres::PoolError;
use derive_more::with_trait::{Display, Error, From};

#[derive(From, Debug, Display)]
pub enum InsertionError {
    QueueNotFound,
    // value will be max_size allowed
    BufferOverflow(i16),
    EmptyData,
    PoolError(PoolError),
    DbError(tokio_postgres::error::Error),
}

impl Error for InsertionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            InsertionError::PoolError(e) => Some(e),
            InsertionError::DbError(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(From, Debug, Display, Error)]
pub enum SyncError {
    Pool(PoolError),
    DbError(tokio_postgres::error::Error),
}
