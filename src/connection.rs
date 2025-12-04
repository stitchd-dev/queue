use crate::command::{Command, Operation};
use crate::constant::{MAX_MESSAGE_SIZE, READ_CHUNK_SIZE, connection_limit, inflight_limit};
use crate::state::AppState;
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
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

        let _conn_permit = match connection_limit().try_acquire() {
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
            let (reader, mut writer) = socker.into_split();

            let mut buf = BytesMut::with_capacity(READ_CHUNK_SIZE);

            let mut reader = BufReader::new(reader);

            while let Ok(bytes_read) = (&mut reader)
                .take(MAX_MESSAGE_SIZE)
                .read(&mut buf)
                .await
                .map(|x| x as u64)
            {
                if bytes_read == 0 {
                    tracing::info!("Client {} disconnected", addr);
                    break;
                }

                if bytes_read == MAX_MESSAGE_SIZE {
                    tracing::error!("Client {} sent too large message", addr);
                    break;
                }

                let permit = match inflight_limit().try_acquire() {
                    Ok(permit) => permit,
                    Err(_) => {
                        writer
                            .write_all(b"Error: Too many in-flight requests")
                            .await
                            .unwrap();
                        continue;
                    }
                };

                let message = Operation::from_bytes(&buf);

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
