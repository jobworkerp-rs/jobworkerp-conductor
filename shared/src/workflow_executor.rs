use anyhow::Result;
use jobworkerp_client::client::helper::JobTerminalOutcome;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_client::jobworkerp::data::{JobId, ResultStatus};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct WorkflowExecutionOutcome {
    pub job_id: Option<JobId>,
    /// Whether the job reached a successful terminal state. A failed-but-executed job still
    /// carries `job_id` so callers can record it for status lookup / cancellation.
    pub success: bool,
    /// Terminal `ResultStatus` observed at execution time. Persisting this on the ExecutionRef
    /// lets the read-time status API distinguish success from failure even when the job left no
    /// stored JobResult (worker `store_failure=false`, or a cancelled PENDING job).
    pub status: ResultStatus,
    pub output: serde_json::Value,
}

impl From<JobTerminalOutcome<serde_json::Value>> for WorkflowExecutionOutcome {
    fn from(outcome: JobTerminalOutcome<serde_json::Value>) -> Self {
        Self {
            success: outcome.is_success(),
            status: outcome.status,
            job_id: outcome.job_id,
            output: outcome.output,
        }
    }
}

/// Execute a workflow (delegates to execute_workflow_with_channel with channel=None)
pub async fn execute_workflow(
    workflow_url: &str,
    jobworkerp_endpoint: &str,
    args: Option<&str>,
) -> Result<()> {
    execute_workflow_with_channel(workflow_url, jobworkerp_endpoint, args, None).await
}

/// Execute a workflow with optional channel
pub async fn execute_workflow_with_channel(
    workflow_url: &str,
    jobworkerp_endpoint: &str,
    args: Option<&str>,
    channel: Option<&str>,
) -> Result<()> {
    tracing::info!(
        "Executing workflow: {} via {} (channel: {:?})",
        workflow_url,
        jobworkerp_endpoint,
        channel
    );

    let client_wrapper = JobworkerpClientWrapper::new(jobworkerp_endpoint, None).await?;

    let input = args.unwrap_or("{}");
    let result = client_wrapper
        .execute_workflow(None, Arc::new(HashMap::new()), workflow_url, input, channel)
        .await?;

    tracing::info!("Workflow execution completed successfully: {:#?}", result);
    Ok(())
}

/// Execute any pre-registered worker by name.
/// Resolves WorkerData at runtime via worker_name (not worker_id, which is unstable).
pub async fn execute_worker_by_name(
    worker_name: &str,
    jobworkerp_endpoint: &str,
    args: Option<&str>,
    using: Option<&str>,
) -> Result<()> {
    tracing::info!(
        "Executing worker by name={} via {} (using: {:?})",
        worker_name,
        jobworkerp_endpoint,
        using
    );
    let client_wrapper = JobworkerpClientWrapper::new(jobworkerp_endpoint, None).await?;
    let args_json: serde_json::Value = match args {
        Some(s) => serde_json::from_str(s)?,
        None => serde_json::json!({}),
    };
    let result = client_wrapper
        .execute_worker_by_name(worker_name, args_json, using)
        .await?;
    tracing::info!("Worker execution completed: {:#?}", result);
    Ok(())
}

/// Streaming-first workflow enqueue: returns the immediate `job_id` (when the runner supports
/// streaming) plus a future resolving to the terminal outcome, so the caller can record the
/// running job before it finishes (see `record_pending_then_update`).
pub async fn execute_workflow_stream_first(
    workflow_url: &str,
    jobworkerp_endpoint: &str,
    args: Option<&str>,
    channel: Option<&str>,
) -> Result<crate::execution_ref_recorder::EnqueuedJob> {
    let client_wrapper = JobworkerpClientWrapper::new(jobworkerp_endpoint, None).await?;
    let input = args.unwrap_or("{}");
    let (job_id, terminal) = client_wrapper
        .execute_workflow_stream_first(workflow_url, input, channel)
        .await?;
    Ok(crate::execution_ref_recorder::EnqueuedJob::from_terminal(
        job_id, terminal,
    ))
}

/// Streaming-first enqueue of a pre-registered worker by name (analogous to
/// `execute_workflow_stream_first`): returns the immediate `job_id` plus the terminal future.
pub async fn execute_worker_by_name_stream_first(
    worker_name: &str,
    jobworkerp_endpoint: &str,
    args: Option<&str>,
    using: Option<&str>,
) -> Result<crate::execution_ref_recorder::EnqueuedJob> {
    let client_wrapper = JobworkerpClientWrapper::new(jobworkerp_endpoint, None).await?;
    // Args may contain user-supplied secrets (API keys, connection strings). Never log content,
    // even at trace — keep to shape info (presence/length) so trace can be enabled in environments
    // where secrets may flow through.
    tracing::trace!(
        worker_name,
        endpoint = jobworkerp_endpoint,
        using = ?using,
        args_present = args.is_some(),
        args_len = args.map(str::len).unwrap_or(0),
        "workflow_executor: execute_worker_by_name_stream_first invoked"
    );
    let args_json: serde_json::Value = match args {
        Some(s) => serde_json::from_str(s).inspect_err(|e| {
            tracing::error!(
                worker_name,
                error = %e,
                "workflow_executor: failed to parse args JSON for worker — args is not valid JSON"
            );
        })?,
        None => serde_json::json!({}),
    };
    tracing::trace!(
        worker_name,
        args_json_kind = match &args_json {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        },
        args_json_top_keys = ?args_json.as_object().map(|m| m.keys().cloned().collect::<Vec<_>>()),
        "workflow_executor: parsed args_json kind / top-level keys"
    );
    let (job_id, terminal) = client_wrapper
        .execute_worker_by_name_stream_first(worker_name, args_json, using)
        .await?;
    Ok(crate::execution_ref_recorder::EnqueuedJob::from_terminal(
        job_id, terminal,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jobworkerp_client::jobworkerp::data::ResultStatus;

    #[test]
    fn outcome_from_terminal_success_keeps_job_id_and_marks_success() {
        let terminal = JobTerminalOutcome {
            job_id: Some(JobId { value: 42 }),
            status: ResultStatus::Success,
            output: serde_json::json!({"ok": true}),
        };
        let outcome: WorkflowExecutionOutcome = terminal.into();
        assert_eq!(outcome.job_id.map(|id| id.value), Some(42));
        assert!(outcome.success);
        assert_eq!(outcome.status, ResultStatus::Success);
        assert_eq!(outcome.output, serde_json::json!({"ok": true}));
    }

    // A terminal failure must still carry job_id so the executed job can be
    // referenced / cancelled via the status API; success must be false.
    #[test]
    fn outcome_from_terminal_failure_keeps_job_id_and_marks_failure() {
        let terminal = JobTerminalOutcome {
            job_id: Some(JobId { value: 7 }),
            status: ResultStatus::ErrorAndRetry,
            output: serde_json::Value::String("boom".to_string()),
        };
        let outcome: WorkflowExecutionOutcome = terminal.into();
        assert_eq!(outcome.job_id.map(|id| id.value), Some(7));
        assert!(!outcome.success);
        // The exact terminal status is preserved so the ExecutionRef can record it.
        assert_eq!(outcome.status, ResultStatus::ErrorAndRetry);
    }
}
