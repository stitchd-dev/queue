use crate::command::{Command, Operation};
use crate::connection_pool::acquire_connection;
use crate::constant::{in_flight_limit, is_valid_message, read_data};
use crate::state::AppState;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub fn process(mut worker_rx: mpsc::Receiver<Command>, state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(command) = worker_rx.recv().await {
            let state = state.clone();

            tokio::spawn(async move {
                command.process(&state).await;
            });
        }
    })
}

pub async fn listen(listener: TcpListener, server_tx: mpsc::Sender<Command>) -> () {
    loop {
        let (mut socker, addr) = listener.accept().await.unwrap();

        let conn_permit = match acquire_connection() {
            Ok(permit) => permit,
            Err(_) => {
                tracing::warn!("Too many active connections.");
                socker
                    .write_all(b"Too many active connections. Please try again later.")
                    .await
                    .unwrap();
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
                    tracing::info!("Client {} disconnected", addr);
                    break;
                }

                if is_valid_message(bytes_read) {
                    tracing::error!("Client {} sent too large message", addr);
                    writer
                        .write_all(b"Error: Message too large. Disconnecting...")
                        .await
                        .unwrap();

                    break;
                }

                let permit = match in_flight_limit().try_acquire() {
                    Ok(permit) => permit,
                    Err(_) => {
                        writer
                            .write_all(b"Error: Too many in-flight requests under process. Please try again later.")
                            .await
                            .unwrap();
                        continue;
                    }
                };

                let message = Operation::read_bytes(buf);

                let operation = match message {
                    Ok(message) => message,
                    Err(e) => {
                        tracing::debug!("Error: {}", e);

                        writer
                            .write_all(format!("Error: {}", e).as_bytes())
                            .await
                            .unwrap();

                        continue;
                    }
                };

                let (tx, rx) = oneshot::channel();

                let cmd = Command {
                    operation,
                    tx,
                    _permit: permit,
                };

                server_tx.send(cmd).await.unwrap();

                let res = rx.await.unwrap();
                writer.write_all(res.as_bytes()).await.unwrap();

                buf.clear();
            }
        });
    }
}
