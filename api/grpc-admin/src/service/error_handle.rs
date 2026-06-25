use infra::error::UiEventHandlerError;
use sqlx::{error::DatabaseError, mysql::MySqlDatabaseError, sqlite::SqliteError};

// TODO map redis etc error
pub fn handle_error(err: &anyhow::Error) -> tonic::Status {
    // TODO search with err.chain()
    match err.downcast_ref::<UiEventHandlerError>() {
        Some(UiEventHandlerError::DBError(sqlx::Error::Database(e))) => map_db_error(e.as_ref()),
        Some(UiEventHandlerError::DBError(sqlx::Error::RowNotFound)) => {
            tracing::warn!("row not found occurred: {:?}", err);
            tonic::Status::not_found(format!("not found: {err:?}"))
        }
        Some(UiEventHandlerError::NotFound(msg)) => tonic::Status::not_found(msg.clone()),
        Some(UiEventHandlerError::AlreadyExists(msg)) => tonic::Status::already_exists(msg.clone()),
        Some(UiEventHandlerError::FailedPrecondition(msg)) => {
            tonic::Status::failed_precondition(msg.clone())
        }
        Some(UiEventHandlerError::Unimplemented(msg)) => tonic::Status::unimplemented(msg.clone()),
        Some(e) => {
            tracing::warn!("unknown error occurred: {:?}", e);
            tonic::Status::internal(format!("unknwon: {e:?}"))
        }
        None => {
            tracing::warn!("other error occurred: {:?}", err);
            tonic::Status::internal(format!("other error: {err:?}"))
        }
    }
}

// TODO あとでちゃんと実装する
fn map_db_error(err: &dyn DatabaseError) -> tonic::Status {
    tracing::warn!("database error: {:?}", err);
    if let Some(e) = err.try_downcast_ref::<SqliteError>() {
        if e.code().as_deref() == Some("2067") {
            // SQLITE_CONSTRAINT_UNIQUE
            tonic::Status::already_exists(format!("{e:?}"))
        } else {
            tracing::warn!("sqlite error occurred: {:?}", e);
            tonic::Status::unavailable(format!("db error: {e:?}"))
        }
    } else if let Some(e) = err.try_downcast_ref::<MySqlDatabaseError>() {
        if e.number() == 1062 {
            // duplicate entry
            tonic::Status::already_exists(format!("{e:?}"))
        } else {
            tracing::warn!("mysql error occurred: {:?}", e);
            tonic::Status::unavailable(format!("db error: {e:?}"))
        }
    } else {
        tracing::warn!("unknown db error occurred: {:?}", err);
        tonic::Status::unavailable(format!("db error: {err:?}"))
    }
}
