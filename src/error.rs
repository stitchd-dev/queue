use deadpool_postgres::PoolError;
use derive_more::with_trait::{Display, Error, From};

#[derive(From, Debug, Display)]
pub enum InsertionError {
    // value will be max_size allowed
    LimitExceeded(usize),
    EmptyData,
}

impl Error for InsertionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}

#[derive(From, Debug, Display, Error)]
pub enum SyncError {
    Pool(PoolError),
    DbError(tokio_postgres::error::Error),
}
