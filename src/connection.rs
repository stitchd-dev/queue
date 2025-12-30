//! TCP connection handling and command processing.
//!
//! This module manages incoming TCP connections, parses commands from clients,
//! and dispatches them to worker tasks for processing. It implements connection
//! pooling and rate limiting to prevent resource exhaustion.

use crate::connection_pool::{acquire_connection, acquire_process};
use crate::constant::{is_valid_message, read_data};
use crate::operation::{Operation, OperationError};
use crate::state::AppState;
use derive_more::{Display, From};
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;

/// Main server loop that accepts and handles TCP connections.
///
/// For each incoming connection:
/// 1. Acquires a connection permit from the pool (rejects if limit reached).
/// 2. Spawns a task to read and process messages from the client.
/// 3. Sends responses back to the client.
///
/// The function runs indefinitely, accepting connections until the listener is closed.
pub(crate) async fn listen(listener: TcpListener, state: Arc<AppState>) -> () {
    loop {
        let state = state.clone();
        let (mut stream, _addr) = match listener.accept().await {
            Ok(sock) => sock,
            Err(e) => {
                tracing::error!("Failed to accept client connection: {}", e);
                continue;
            }
        };

        let conn_permit = match acquire_connection() {
            Ok(permit) => permit,
            Err(_) => {
                tracing::warn!("Too many active connections.");
                send_response(&mut stream, b"Error: Too many active connections.").await;
                continue;
            }
        };

        tokio::spawn(async move {
            let mut conn_permit = conn_permit;
            let buf = conn_permit.bytes();

            let (reader, mut writer) = stream.into_split();

            let mut reader = BufReader::new(reader);

            while let Ok(bytes_read) = read_data(&mut reader, buf).await {
                if bytes_read == 0 {
                    break;
                }

                if !is_valid_message(bytes_read) {
                    send_response(&mut writer, b"Error: Message too large. Disconnecting...").await;

                    break;
                }

                let res = process_bytes(buf.as_ref(), state.clone()).await;

                match res {
                    Ok(res) => send_response(&mut writer, res.as_bytes()).await,
                    Err(e) => send_error_response(&mut writer, e).await,
                }

                buf.clear();
            }
        });
    }
}

/// Sends an error response to the client based on the error type.
///
/// Converts `ProcessError` variants into human-readable error messages
/// and writes them to the client connection.
async fn send_error_response(mut writer: &mut OwnedWriteHalf, e: ProcessError) {
    let message = match e {
        ProcessError::LimitsExceeded => {
            "Error: Too many in-flight requests under process. Please try again later.".to_string()
        }
        ProcessError::InvalidInput(e) => match e {
            OperationError::OperationNotFound => "Error: Operation not found.".to_string(),
            OperationError::InvalidPayload => "Error: Empty Payload.".to_string(),
            OperationError::ChunkSizeExceeded(err) => {
                format!("Error: Chunk size exceeded: {}", err)
            }
            OperationError::DeserializationError(err) => {
                format!("Error: Deserialization error: {}", err)
            }
            OperationError::UTFError(err) => format!("Error: UTF error: {}", err),
            OperationError::IntParse(err) => format!("Error: Int parse error: {}", err),
            OperationError::QueueNotFound => "Error: Queue not found.".to_string(),
        },
    };

    send_response(&mut writer, message.as_bytes()).await;
}

/// Writes a response to the client connection.
///
/// Logs an error if the write fails but does not propagate the error.
async fn send_response<T: AsyncWriteExt + Unpin>(writer: &mut T, bytes: &[u8]) -> () {
    if let Err(e) = writer.write_all(bytes).await {
        tracing::error!("Failed to write to client: {}", e);
    }
}

/// Error type for command processing operations.
#[derive(Debug, Display, From)]
pub enum ProcessError {
    /// Processing limit exceeded (too many in-flight requests).
    LimitsExceeded,
    /// Invalid input or operation error.
    InvalidInput(OperationError),
}

/// Processes raw bytes from a client connection into a command.
///
/// Steps:
/// 1. Acquires a processing permit (rate limiting).
/// 2. Parses the bytes into an `Operation`.
/// 3. Processes the operation and returns the response.
pub async fn process_bytes(bytes: &[u8], state: Arc<AppState>) -> Result<String, ProcessError> {
    let _permit = match acquire_process() {
        Ok(permit) => permit,
        Err(_) => {
            return Err(ProcessError::LimitsExceeded);
        }
    };

    let operation = Operation::read_bytes(bytes, state.as_ref()).await?;

    let result = match operation {
        Operation::Ping => {
            tracing::debug!("Client pinged");
            "Pong".to_string()
        }
        Operation::Insert(queue_id, message) => {
            tracing::debug!("Inserting data");
            let message = message
                .into_iter()
                .map(|v| serde_json::to_vec(&v).unwrap())
                .collect();
            match state.insert_data(queue_id, message).await {
                Ok(()) => "OK".to_string(),
                Err(e) => format!("Error: {}", e),
            }
        }
    };

    Ok(result)
}
