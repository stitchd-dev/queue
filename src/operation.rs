//! Command parsing and operation handling.
//!
//! This module implements a binary protocol parser for client-server communication.
//! It handles parsing raw byte streams into structured operations and validates them
//! against the application state. Due to the sensitive nature of binary parsing and
//! the security implications of malformed input, this module employs strict validation
//! at every parsing step.
//!
//! # Protocol Overview
//!
//! The protocol is a binary format where each message begins with a single command byte
//! followed by command-specific payload data. All multi-byte integers use big-endian
//! (network byte order) encoding.
//!
//! ## Supported Commands
//!
//! ### Ping Command (0x00)
//!
//! **Purpose**: Health check to verify server availability.
//!
//! **Binary Format**:
//! ```text
//! +--------+
//! | 0x00   |  Command byte
//! +--------+
//! Total: 1 byte
//! ```
//!
//! **Response**: "Pong"
//!
//! ### Insert Command (0x01)
//!
//! **Purpose**: Insert one or more event payloads into a specific queue.
//!
//! **Binary Format**:
//! ```text
//! +--------+------------------+----------------------+
//! | 0x01   | queue_id (4B)    | CBOR array of values |
//! +--------+------------------+----------------------+
//! Command  Queue ID (i32 BE)  CBOR-encoded payload array
//! ```
//!
//! **Field Descriptions**:
//! - `command_byte` (1 byte): Must be `0x01`
//! - `queue_id` (4 bytes): Target queue identifier as big-endian i32
//! - `cbor_array` (variable): CBOR-encoded array where each element is any CBOR value
//!
//! **CBOR Payload Format**:
//! The payloads are encoded as a CBOR array where each element can be **any valid CBOR value**.
//! This includes byte strings, text strings, integers, arrays, maps, and other CBOR types.
//! Each element is stored as its raw CBOR-encoded representation.
//!
//! Examples:
//! - `[bytes_1, bytes_2, ...]` - Array of byte strings
//! - `["event1", "event2"]` - Array of text strings  
//! - `[{"key": 1}, {"key": 2}]` - Array of CBOR maps
//! - `[[1, 2], [3, 4]]` - Array of CBOR arrays
//!
//! **Constraints**:
//! - Queue ID must exist in the system
//! - CBOR array must contain at least one element (cannot be empty)
//! - Each payload element can be any CBOR value type
//! - Each payload's encoded size must be non-empty (length > 0)
//! - Each payload's encoded size must not exceed 65,535 bytes (64 KB limit)
//! - Total message size must not exceed system limits (enforced at connection layer)
//!
//! # Parsing Strategy
//!
//! The parser uses a defensive, fail-fast approach:
//! 1. **Bounds checking**: Every slice operation uses checked methods to prevent panics
//! 2. **Early validation**: Invalid states are detected as soon as possible
//! 3. **No partial success**: Either the entire message parses successfully or an error is returned
//! 4. **State validation**: Queue existence is verified before accepting the operation
//!
//! # Security Considerations
//!
//! This module is a critical security boundary. Malformed or malicious input could:
//! - Cause buffer overflows (prevented by checked slice operations)
//! - Trigger panics (prevented by Result-based error handling)
//! - Exhaust memory (prevented by size limits at connection layer)
//! - Reference non-existent queues (prevented by state validation)
//!
//! All parsing operations are designed to be safe even with adversarial input.
//!
//! # Error Handling
//!
//! Parsing errors are categorized into:
//! - `OperationNotFound`: Unknown command byte
//! - `InvalidPayload`: Malformed message structure or constraint violation
//! - `QueueNotFound`: Valid message but references non-existent queue
//! - `SliceError`: Internal conversion error (wrapped from TryFromSliceError)
//!
//! # Examples
//!
//! ## Ping Command
//! ```text
//! Bytes: [0x00]
//! Result: Operation::Ping
//! ```
//!
//! ## Insert Command (Byte String Payloads)
//! ```text
//! Bytes: [0x01, 0x00, 0x00, 0x00, 0x2A, 0x81, 0x4D, b'{', b'"', b'k', b'e', b'y', b'"', b':', b'1', b'}']
//!        [cmd ] [queue_id = 42      ] [CBOR: array(1), bytes(13), JSON payload: {"key":1}    ]
//! Result: Operation::Insert(42, vec![<CBOR-encoded bytes>])
//!
//! CBOR breakdown:
//! - 0x81: CBOR array with 1 element
//! - 0x4D: CBOR byte string with length 13
//! - Following 13 bytes: the actual payload data
//! ```
//!
//! ## Insert Command (Text String Payloads)
//! ```text
//! Bytes: [0x01, 0x00, 0x00, 0x00, 0x01, 0x82, 0x65, b'e', b'v', b'e', b'n', b't', 0x64, b'd', b'a', b't', b'a']
//!        [cmd ] [queue_id = 1       ] [CBOR: array(2), text(5) "event", text(4) "data"]
//! Result: Operation::Insert(1, vec![<CBOR-encoded "event">, <CBOR-encoded "data">])
//!
//! CBOR breakdown:
//! - 0x82: CBOR array with 2 elements
//! - 0x65: CBOR text string with length 5, followed by "event"
//! - 0x64: CBOR text string with length 4, followed by "data"
//! ```
//!
//! ## Insert Command (Mixed CBOR Types)
//! ```text
//! Events can be any CBOR value: integers, maps, arrays, etc.
//! Each element is stored as its raw CBOR-encoded bytes.
//! ```

use crate::state::AppState;
use bytes::Buf;
use cbor_data::{ParseError, TypeError};
use derive_more::{Display, From};
use std::array::TryFromSliceError;

/// Error type for operation parsing and validation.
///
/// This enum represents all possible failure modes during command parsing.
/// Each variant indicates a specific category of error that helps clients
/// understand what went wrong and how to fix their request.
///
/// # Variants
///
/// ## `OperationNotFound`
/// The command byte does not match any known operation (not 0x00 or 0x01).
/// This typically indicates:
/// - Client is using an unsupported protocol version
/// - Corrupted data transmission
/// - Client implementation bug
///
/// **Recovery**: Client should verify they're sending the correct command byte.
///
/// ## `InvalidPayload`
/// The message structure is malformed or violates protocol constraints.
/// This can occur when:
/// - Message is too short (missing required fields)
/// - Payload count is zero
/// - Individual payload length is zero
/// - Declared payload count doesn't match actual payloads
/// - Message ends prematurely (truncated data)
/// - Payload count and actual payload mismatch (too many or too few)
///
/// **Recovery**: Client should verify message construction and ensure all
/// length fields accurately reflect the data being sent.
///
/// ## `SliceError`
/// Internal conversion error when parsing fixed-size fields (queue_id, lengths).
/// This is typically wrapped from `TryFromSliceError` and indicates the slice
/// size doesn't match the expected type size. Should not occur if InvalidPayload
/// checks are working correctly, but provides an additional safety layer.
///
/// **Recovery**: This usually indicates a programming error in the parser itself.
///
/// ## `QueueNotFound`
/// The message is well-formed, but the specified queue_id doesn't exist in the
/// system. This can happen when:
/// - Queue was deleted after client cached the ID
/// - Client is using an incorrect/outdated queue ID
/// - Queue hasn't been activated yet
///
/// **Recovery**: Client should refresh their queue list and verify the queue
/// exists before retrying.
///
/// ## `CBORParse`
/// CBOR parsing error occurred while decoding the payload array.
/// This indicates the CBOR data is malformed or corrupted. Common causes:
/// - Invalid CBOR encoding (not following RFC 8949)
/// - Truncated CBOR data (incomplete message)
/// - Corrupted bytes during transmission
///
/// **Recovery**: Client should verify CBOR encoding is correct and complete.
///
/// ## `CBORType`
/// CBOR type mismatch error - the CBOR structure doesn't match expected types.
/// This occurs when:
/// - Expected a CBOR array but got a different type (map, integer, etc.)
/// - CBOR structure is valid but semantically incorrect for this protocol
///
/// **Recovery**: Client should ensure payloads are encoded as a CBOR array of values.
#[derive(Debug, Display, From)]
pub enum OperationError {
    /// Operation type not recognized (invalid command byte).
    OperationNotFound,
    /// Payload is invalid, empty, or violates protocol constraints.
    InvalidPayload,
    /// Internal slice conversion error (should not occur with proper validation).
    SliceError(TryFromSliceError),
    /// Queue ID not found in the system.
    QueueNotFound,
    /// CBOR parsing error (malformed or corrupted CBOR data).
    CBORParse(ParseError),
    /// CBOR type error (structure doesn't match expected types).
    CBORType(TypeError),
}

/// Represents a parsed operation from a client command.
///
/// This enum encapsulates the result of successfully parsing a binary protocol message.
/// Each variant corresponds to a specific command type and carries the parsed data
/// needed to execute that command.
///
/// # Variants
///
/// ## `Ping`
/// A health check operation with no associated data. Used by clients to verify
/// the server is responsive. The server responds with "Pong".
///
/// **Use case**: Connection health monitoring, keepalive checks.
///
/// ## `Insert(i32, Vec<Vec<u8>>)`
/// A data insertion operation containing:
/// - `i32`: The target queue ID (validated to exist)
/// - `Vec<Vec<u8>>`: Vector of CBOR-encoded payloads
///
/// Each inner `Vec<u8>` represents one complete CBOR-encoded payload. The outer vector contains
/// all payloads from a single Insert command. Each payload can be any CBOR value type
/// (byte strings, text strings, integers, arrays, maps, etc.) stored as its raw CBOR encoding.
///
/// **Use case**: Batch insertion of events into a specific queue for processing.
///
/// # Invariants
///
/// When an `Operation` is successfully constructed:
/// - For `Ping`: No invariants (always valid)
/// - For `Insert(queue_id, payloads)`:
///   - `queue_id` exists in the system (verified during parsing)
///   - `payloads` is non-empty (at least 1 item)
///   - Each payload in `payloads` is non-empty (1-65535 bytes)
///
/// These invariants are guaranteed by the `read_bytes` parser.
pub enum Operation {
    /// Ping command to check server availability.
    Ping,
    /// Insert command with queue ID and CBOR-encoded payloads.
    ///
    /// Fields:
    /// - `0`: Queue ID (i32) - validated to exist in the system
    /// - `1`: Payloads (Vec<Vec<u8>>) - non-empty vector of CBOR-encoded values (1-65535 bytes each)
    Insert(i32, Vec<Vec<u8>>),
}

impl Operation {
    /// Parses a binary protocol message into an Operation.
    ///
    /// This is the main entry point for parsing client commands. It reads the command byte
    /// and dispatches to the appropriate parser based on the operation type.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Raw byte slice containing the complete message (command byte + payload)
    /// * `state` - Application state for validating queue existence
    ///
    /// # Returns
    ///
    /// * `Ok(Operation)` - Successfully parsed operation with all invariants satisfied
    /// * `Err(OperationError)` - Parsing failed due to invalid format, unknown command, or validation error
    ///
    /// # Protocol Flow
    ///
    /// 1. Check if bytes are non-empty
    /// 2. Read command byte (first byte)
    /// 3. Dispatch to appropriate parser:
    ///    - `0x00` → Return `Operation::Ping` immediately
    ///    - `0x01` → Parse Insert command with remaining bytes
    ///    - Other → Return `OperationNotFound` error
    ///
    /// # Errors
    ///
    /// * `InvalidPayload` - Empty byte slice
    /// * `OperationNotFound` - Unknown command byte (not 0x00 or 0x01)
    /// * Other errors propagated from `parse_insert` for Insert commands
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Ping command
    /// let bytes = &[0x00];
    /// let op = Operation::read_bytes(bytes, &state).await?;
    /// assert!(matches!(op, Operation::Ping));
    ///
    /// // Insert command (simplified)
    /// let bytes = &[0x01, 0x00, 0x00, 0x00, 0x01, /* CBOR data */];
    /// let op = Operation::read_bytes(bytes, &state).await?;
    /// ```
    pub(crate) async fn read_bytes(
        mut bytes: &[u8],
        state: &AppState,
    ) -> Result<Self, OperationError> {
        if !bytes.has_remaining() {
            return Err(OperationError::InvalidPayload);
        }

        match bytes.get_u8() {
            0x00 => Ok(Self::Ping),
            0x01 => Self::parse_insert(bytes, state).await,
            _ => Err(OperationError::OperationNotFound),
        }
    }

    /// Parses an Insert command from the remaining bytes after the command byte.
    ///
    /// This method handles the complex parsing of Insert operations, including:
    /// - Extracting the queue ID
    /// - Validating queue existence
    /// - Parsing CBOR-encoded payload array
    /// - Extracting raw CBOR encoding of each element
    /// - Validating individual payload constraints
    ///
    /// # Arguments
    ///
    /// * `bytes` - Remaining bytes after the command byte (queue_id + CBOR data)
    /// * `state` - Application state for queue validation
    ///
    /// # Returns
    ///
    /// * `Ok(Operation::Insert(queue_id, payloads))` - Successfully parsed with all validations passed
    /// * `Err(OperationError)` - Parsing or validation failed
    ///
    /// # Protocol Structure
    ///
    /// ```text
    /// [queue_id: 4 bytes][CBOR array of any CBOR values: variable length]
    /// ```
    ///
    /// # Parsing Steps
    ///
    /// 1. **Header validation**: Ensure at least 5 bytes (4 for queue_id + 1 minimum for CBOR)
    /// 2. **Queue ID extraction**: Read 4-byte big-endian i32
    /// 3. **Queue validation**: Verify queue exists in system state
    /// 4. **CBOR parsing**: Parse remaining bytes as CBOR array
    /// 5. **Payload extraction**: Extract each element's raw CBOR encoding (supports any CBOR type)
    /// 6. **Size validation**: Check each payload's encoded size is 1-65535 bytes
    /// 7. **Count validation**: Ensure at least one payload exists
    ///
    /// # Errors
    ///
    /// * `InvalidPayload` - Message too short, empty array, invalid payload size, or empty payload
    /// * `QueueNotFound` - Queue ID doesn't exist in the system
    /// * `CBORParse` - CBOR data is malformed or corrupted
    /// * `CBORType` - CBOR structure doesn't match expected types (not an array)
    ///
    /// # Invariants Guaranteed
    ///
    /// On success, the returned `Operation::Insert(queue_id, payloads)` guarantees:
    /// - `queue_id` exists in the system
    /// - `payloads` contains at least 1 element
    /// - Each payload is 1-65535 bytes (non-empty, max 64 KB)
    /// - Each payload contains valid CBOR-encoded data (any CBOR type)
    async fn parse_insert(mut bytes: &[u8], state: &AppState) -> Result<Self, OperationError> {
        // Header: Queue ID (4 bytes) + Payload Count (1 byte)
        if bytes.remaining() < 5 {
            return Err(OperationError::InvalidPayload);
        }

        let queue_id = bytes.get_i32();

        if !state.check_if_queue_exists(queue_id) {
            return Err(OperationError::QueueNotFound);
        }

        let data = cbor_data::Cbor::checked(bytes)?.try_array()?;

        let res: Vec<Vec<u8>> = data
            .into_iter()
            .map(|item| {
                let bytes = item.as_ref().as_ref();

                if bytes.is_empty() || bytes.len() > u16::MAX as usize {
                    Err(OperationError::InvalidPayload)
                } else {
                    Ok(bytes.to_vec())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        if res.is_empty() {
            return Err(OperationError::InvalidPayload);
        }

        Ok(Self::Insert(queue_id, res))
    }
}
