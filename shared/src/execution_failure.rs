//! Classification of execution errors for the conductor's retry decision.
//!
//! jobworkerp retries failed jobs itself (per the worker's RetryPolicy), so the conductor must
//! not re-submit a job that was created: doing so duplicates the execution. Only a failure to
//! reach jobworkerp before any job exists is worth retrying here.

use jobworkerp_client::error::ClientError;
use std::fmt;

/// Context marker for an error raised after jobworkerp assigned a job_id. The job exists and may
/// still be running (e.g. its result stream was cut off), so it must not be re-submitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobAlreadyEnqueued {
    pub job_id: i64,
}

impl fmt::Display for JobAlreadyEnqueued {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "job {} was enqueued but its terminal result could not be obtained",
            self.job_id
        )
    }
}

/// Whether re-submitting after `err` is safe and useful: no job was created and the cause is a
/// connection-level failure that may clear up on its own.
pub fn is_retriable_connection_failure(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<JobAlreadyEnqueued>().is_some() {
        return false;
    }
    err.chain().any(is_connection_cause)
}

fn is_connection_cause(cause: &(dyn std::error::Error + 'static)) -> bool {
    if cause.is::<tonic::transport::Error>() {
        return true;
    }
    if let Some(status) = cause.downcast_ref::<tonic::Status>() {
        return status.code() == tonic::Code::Unavailable;
    }
    // jobworkerp-client folds Unavailable into this variant and drops the original status. It
    // also folds Unauthenticated / PermissionDenied / ResourceExhausted into it, so those are
    // retried too: a few extra attempts are harmless, whereas skipping Unavailable would give up
    // on a server that is briefly unreachable.
    matches!(
        cause.downcast_ref::<ClientError>(),
        Some(ClientError::ExternalServiceError(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn real_connect_error() -> anyhow::Error {
        // Loopback + a closed port: the connection is refused immediately, exactly as when the
        // conductor cannot reach a jobworkerp server.
        let err = tonic::transport::Endpoint::from_static("http://127.0.0.1:1")
            .connect()
            .await
            .expect_err("connecting to a closed port must fail");
        anyhow::Error::new(err).context("Failed to connect to gRPC server at http://127.0.0.1:1")
    }

    #[tokio::test]
    async fn connect_failure_is_retriable() {
        assert!(is_retriable_connection_failure(&real_connect_error().await));
    }

    #[test]
    fn unavailable_status_is_retriable() {
        let err = anyhow::Error::new(tonic::Status::unavailable("tcp connect error"))
            .context("enqueue failed");
        assert!(is_retriable_connection_failure(&err));
    }

    #[test]
    fn external_service_client_error_is_retriable() {
        let err = anyhow::Error::new(ClientError::ExternalServiceError(
            "transport error".to_string(),
        ));
        assert!(is_retriable_connection_failure(&err));
    }

    #[tokio::test]
    async fn already_enqueued_job_is_never_retried() {
        let marker = JobAlreadyEnqueued { job_id: 42 };
        assert!(!is_retriable_connection_failure(
            &real_connect_error().await.context(marker)
        ));
    }

    #[test]
    fn non_connection_failures_are_not_retriable() {
        let errors = [
            // Only typed causes count; wording alone is not a connection failure.
            anyhow::anyhow!("Connection refused"),
            anyhow::Error::new(tonic::Status::invalid_argument("bad args")),
            // A timeout may fire after the job was created, so re-submitting could duplicate it.
            anyhow::Error::new(ClientError::TimeoutError("deadline".to_string())),
            anyhow::Error::new(ClientError::NotFound("worker".to_string())),
            anyhow::anyhow!("Job for handler 'h' reached a failed terminal state: boom"),
        ];
        for err in errors {
            assert!(!is_retriable_connection_failure(&err), "{err:#}");
        }
    }

    #[test]
    fn marker_is_displayed_with_its_job_id() {
        let err = anyhow::anyhow!("result stream ended before End")
            .context(JobAlreadyEnqueued { job_id: 7 });
        assert_eq!(
            err.to_string(),
            "job 7 was enqueued but its terminal result could not be obtained"
        );
    }
}
