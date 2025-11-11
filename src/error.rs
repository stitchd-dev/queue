#[derive(Debug)]
pub enum InsertionError {
    QueueNotFound,
    // value will be max_size allowed
    BufferOverflow(i16),
    EmptyData,
}