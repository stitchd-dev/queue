use crate::constant::{is_insert, is_ping, is_valid_chunk, min_len_check};
use derive_more::{Display, From};
use serde_json::{Deserializer, Value};
use tokio::sync::{SemaphorePermit, oneshot};

#[derive(Debug, Display, From)]
pub enum OperationError {
    OperationNotFound,
    InvalidPayload,
    ChunkSizeExceeded(usize),
    DeserializationError(DeSerializationError),
}

#[derive(Debug, Display)]
#[display("Error serializing chunk {}: {}", index, error)]
pub struct DeSerializationError {
    index: usize,
    error: serde_json::Error,
}

pub enum Operation {
    Ping,
    Insert(Vec<Value>),
}

impl Operation {
    pub(crate) fn read_bytes(bytes: &[u8]) -> Result<Self, OperationError> {
        let bytes = bytes.trim_ascii();
        if min_len_check(bytes) {
            return Err(OperationError::OperationNotFound);
        }
        if is_ping(bytes) {
            Ok(Self::Ping)
        } else if let Some(bytes) = is_insert(bytes) {
            let bytes = bytes.trim_ascii();

            if bytes.is_empty() {
                Err(OperationError::InvalidPayload)
            } else {
                let mut result = Vec::new();
                let mut stream = Deserializer::from_slice(bytes).into_iter::<Value>();

                let mut prev_offset = 0;
                let mut index = 0;

                while let Some(value) = stream.next() {
                    let value = value.map_err(|e| {
                        OperationError::DeserializationError(DeSerializationError {
                            index,
                            error: e,
                        })
                    })?;

                    let current_offset = stream.byte_offset();

                    let size = current_offset - prev_offset;

                    if is_valid_chunk(size) {
                        prev_offset = current_offset;
                        result.push(value);
                        index += 1;
                    } else {
                        return Err(OperationError::ChunkSizeExceeded(index));
                    }
                }

                Ok(Self::Insert(result))
            }
        } else {
            Err(OperationError::OperationNotFound)
        }
    }
}

pub struct Command {
    pub(crate) operation: Operation,
    pub(crate) tx: oneshot::Sender<String>,
    pub(crate) _permit: SemaphorePermit<'static>,
}

impl Command {
    pub async fn process(self, state: &crate::AppState) {
        match self.operation {
            Operation::Ping => {
                tracing::debug!("Client pinged");

                match self.tx.send("Pong".to_string()) {
                    Ok(()) => (),
                    Err(err) => tracing::warn!("Failed to send ping response: {}", err),
                };
            }
            Operation::Insert(message) => {
                tracing::debug!("Inserting data");

                match self.tx.send(match state.insert_data(1, message).await {
                    Ok(()) => "OK".to_string(),
                    Err(e) => format!("Error: {}", e),
                }) {
                    Ok(()) => (),
                    Err(err) => tracing::warn!("Failed to send ping response: {}", err),
                };
            }
        }
    }
}
