use crate::constant::{is_insert, is_ping, is_valid_chunk, min_len_check};
use crate::state::AppState;
use derive_more::{Display, From};
use serde_json::{Deserializer, Value};
use std::num::ParseIntError;
use std::str::Utf8Error;
use tokio::sync::{SemaphorePermit, oneshot};

#[derive(Debug, Display, From)]
pub enum OperationError {
    OperationNotFound,
    InvalidPayload,
    ChunkSizeExceeded(usize),
    DeserializationError(DeSerializationError),
    UTFError(Utf8Error),
    IntParse(ParseIntError),
    QueueNotFound,
}

#[derive(Debug, Display)]
#[display("Error serializing chunk {}: {}", index, error)]
pub struct DeSerializationError {
    index: usize,
    error: serde_json::Error,
}

pub enum Operation {
    Ping,
    Insert(i32, Vec<Value>),
}

impl Operation {
    pub(crate) async fn read_bytes(bytes: &[u8], state: &AppState) -> Result<Self, OperationError> {
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
                // Find space within first 11 bytes (max i32 digits + sign)
                // i32 range is -2,147,483,648 to 2,147,483,647 (max 11 chars including sign)
                let search_limit = bytes.len().min(12); // +1 for the space
                let space_pos = bytes[..search_limit]
                    .iter()
                    .position(|&b| b == b' ')
                    .ok_or(OperationError::InvalidPayload)?;

                // Extract queue_id bytes
                let queue_id_bytes = &bytes[..space_pos];
                let queue_id_str = std::str::from_utf8(queue_id_bytes)?;
                let queue_id: i32 = queue_id_str.parse()?;

                if !state.check_if_queue_exists(queue_id).await {
                    return Err(OperationError::QueueNotFound);
                }

                // Extract Payload bytes
                let bytes = bytes[space_pos + 1..].trim_ascii();

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

                Ok(Self::Insert(queue_id, result))
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
            Operation::Insert(queue_id, message) => {
                tracing::debug!("Inserting data");

                match self
                    .tx
                    .send(match state.insert_data(queue_id, message).await {
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
