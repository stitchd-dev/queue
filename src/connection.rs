use crate::command::{Command, Operation, OperationError};
use crate::connection_pool::{acquire_connection, acquire_process};
use crate::constant::{EVENT_PROCESSOR_MPSC_BUFFER_LIMIT, is_valid_message, read_data};
use crate::state::AppState;
use derive_more::{Display, From};
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

fn process_worker(mut worker_rx: mpsc::Receiver<Command>, state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(command) = worker_rx.recv().await {
            let state = state.clone();

            tokio::spawn(async move {
                command.process(&state).await;
            });
        }
    })
}

pub(crate) async fn listen(listener: TcpListener, state: Arc<AppState>) -> () {
    let (server_tx, worker_rx) = mpsc::channel::<Command>(EVENT_PROCESSOR_MPSC_BUFFER_LIMIT);

    let _worker_handle = process_worker(worker_rx, state.clone());

    loop {
        let state = state.clone();
        let (mut socker, _addr) = match listener.accept().await {
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
                send_response(&mut socker, b"Error: Too many active connections.").await;
                continue;
            }
        };

        let server_tx = server_tx.clone();
        tokio::spawn(async move {
            let mut conn_permit = conn_permit;
            let buf = conn_permit.bytes();

            let (reader, mut writer) = socker.into_split();

            let mut reader = BufReader::new(reader);

            while let Ok(bytes_read) = read_data(&mut reader, buf).await {
                if bytes_read == 0 {
                    break;
                }

                if !is_valid_message(bytes_read) {
                    send_response(&mut writer, b"Error: Message too large. Disconnecting...").await;

                    break;
                }

                let res = process_bytes(&server_tx, buf.as_ref(), &state).await;

                match res {
                    Ok(res) => send_response(&mut writer, res.as_bytes()).await,
                    Err(e) => send_error_response(&mut writer, e).await,
                }

                buf.clear();
            }
        });
    }
}

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
        ProcessError::Send(err) => {
            tracing::error!("Failed to send command to worker: {}", err);

            "InternalError: Failed to send event to processor.".to_string()
        }
        ProcessError::Receiver(err) => {
            tracing::error!("Failed to receive command from worker: {}", err);

            "InternalError: Failed to receive event from processor.".to_string()
        }
    };

    send_response(&mut writer, message.as_bytes()).await;
}

async fn send_response<T: AsyncWriteExt + Unpin>(writer: &mut T, bytes: &[u8]) -> () {
    if let Err(e) = writer.write_all(bytes).await {
        tracing::error!("Failed to write to client: {}", e);
    }
}

#[derive(Debug, Display, From)]
pub enum ProcessError {
    LimitsExceeded,
    InvalidInput(OperationError),
    Send(SendError<Command>),
    Receiver(oneshot::error::RecvError),
}

pub async fn process_bytes(
    sender: &mpsc::Sender<Command>,
    bytes: &[u8],
    state: &AppState,
) -> Result<String, ProcessError> {
    let permit = match acquire_process() {
        Ok(permit) => permit,
        Err(_) => {
            return Err(ProcessError::LimitsExceeded);
        }
    };

    let operation = Operation::read_bytes(bytes, state).await?;

    let (tx, rx) = oneshot::channel();

    let cmd = Command {
        operation,
        tx,
        _permit: permit,
    };

    sender.send(cmd).await?;

    Ok(rx.await?)
}
