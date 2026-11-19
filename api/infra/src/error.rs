use prost::DecodeError;
use redis::RedisError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UiEventHandlerError {
    #[error("TonicServerError({0:?})")]
    TonicServerError(tonic::transport::Error),
    #[error("RuntimeError({0})")]
    RuntimeError(String),
    #[error("CodecError({0:?})")]
    CodecError(DecodeError),
    #[error("NotFound({0})")]
    NotFound(String),
    #[error("AlreadyExists({0})")]
    AlreadyExists(String),
    #[error("FailedPrecondition({0})")]
    FailedPrecondition(String),
    #[error("Unimplemented({0})")]
    Unimplemented(String),
    #[error("RedisError({0:?})")]
    RedisError(RedisError),
    #[error("DBError({0:?})")]
    DBError(sqlx::Error),
    #[error("GenerateIdError({0})")]
    GenerateIdError(String),
    #[error("ParseError({0})")]
    ParseError(String),
    #[error("OtherError({0})")]
    OtherError(String),
}
impl From<tonic::transport::Error> for UiEventHandlerError {
    fn from(e: tonic::transport::Error) -> Self {
        UiEventHandlerError::TonicServerError(e)
    }
}
impl From<RedisError> for UiEventHandlerError {
    fn from(e: RedisError) -> Self {
        UiEventHandlerError::RedisError(e)
    }
}

/// Whether a write failed on a unique index. Matching vendor codes misses
/// MySQL, whose `code()` is the SQLSTATE (23000), so ask the driver instead.
pub fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db_error) if db_error.is_unique_violation())
}
