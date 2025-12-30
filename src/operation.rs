//! Command parsing and operation handling.
//!
//! This module defines the command protocol for client interactions,
//! including parsing raw bytes into operations and executing them.

use crate::state::AppState;
use derive_more::{Display, From};
use std::array::TryFromSliceError;

/// Error type for operation parsing and validation.
#[derive(Debug, Display, From)]
pub enum OperationError {
    /// Operation type not recognized.
    OperationNotFound,
    /// Payload is invalid or empty.
    InvalidPayload,
    SliceError(TryFromSliceError),
    /// Queue ID not found in the system.
    QueueNotFound,
}

/// Represents a parsed operation from a client command.
pub enum Operation {
    /// Ping command to check server availability.
    Ping,
    /// Insert command with queue ID and JSON payloads.
    Insert(i32, Vec<Vec<u8>>),
}

impl Operation {
    /// Parses raw bytes into an `Operation`.
    ///
    /// Supported commands:
    /// - `ping` - Returns `Operation::Ping`
    /// - `insert <queue_id> <json_payloads>` - Returns `Operation::Insert` with parsed data
    ///
    /// # Errors
    /// Returns `OperationError` if:
    /// - The command is not recognized
    /// - The payload is invalid or empty
    /// - The queue ID doesn't exist
    pub(crate) async fn read_bytes(bytes: &[u8], state: &AppState) -> Result<Self, OperationError> {
        let (command_byte, payload) = bytes.split_first().ok_or(OperationError::InvalidPayload)?;

        match command_byte {
            0x00 => {
                // Ping
                Ok(Self::Ping)
            }
            0x01 => {
                // Insert
                let (queue_id, payload) = payload
                    .split_at_checked(4)
                    .ok_or(OperationError::InvalidPayload)?;

                let queue_id = i32::from_be_bytes(queue_id.try_into()?);

                if !state.check_if_queue_exists(queue_id) {
                    return Err(OperationError::QueueNotFound);
                }
                let (buf_length, mut payload) = payload
                    .split_first()
                    .ok_or(OperationError::InvalidPayload)?;

                let buf_length = buf_length.clone() as usize;

                if buf_length == 0 {
                    return Err(OperationError::InvalidPayload);
                }

                let mut result: Vec<Vec<u8>> = Vec::with_capacity(buf_length);
                while !payload.is_empty() {
                    if buf_length == result.len() {
                        return Err(OperationError::InvalidPayload);
                    }

                    let (payload_length, data) = payload
                        .split_at_checked(2)
                        .ok_or(OperationError::InvalidPayload)?;

                    let payload_length = u16::from_be_bytes(payload_length.try_into()?);

                    if payload_length == 0 {
                        return Err(OperationError::InvalidPayload);
                    }

                    let (res, remainder) = data
                        .split_at_checked(payload_length as usize)
                        .ok_or(OperationError::InvalidPayload)?;

                    payload = remainder;

                    result.push(res.to_vec());
                }

                if result.is_empty() {
                    return Err(OperationError::InvalidPayload);
                }

                Ok(Operation::Insert(queue_id, result))
            }
            _ => Err(OperationError::OperationNotFound),
        }
    }
}
