// Handler Executor
// Executes workflows for matched Slack event handlers

use anyhow::{Context, Result};
use proto::jobworkerp_conductor::data::slack_event_handler_data::ExecutionTarget;
use proto::jobworkerp_conductor::data::{
    ExecutionRef, ExecutionSourceType, JobworkerpServerId, SlackEventHandler,
};
use serde_json::json;
use shared::SharedLocalConfigStore;
use std::time::Duration;

/// Handler Executor
/// Executes workflows using workflow_executor from jobworkerp-conductor
pub struct HandlerExecutor {
    local_config_store: SharedLocalConfigStore,
    execution_ref_recorder: shared::SharedExecutionRefRecorder,
}

impl HandlerExecutor {
    const MAX_ATTEMPTS: u32 = 3;
    /// Create new HandlerExecutor
    pub fn new(
        local_config_store: SharedLocalConfigStore,
        execution_ref_recorder: shared::SharedExecutionRefRecorder,
    ) -> Self {
        Self {
            local_config_store,
            execution_ref_recorder,
        }
    }

    /// Execute workflow with retry strategy
    /// - Only connection failures before a job is created are retried (see
    ///   [`shared::execution_failure::is_retriable_connection_failure`]); a created job's failure
    ///   is retried by jobworkerp per its RetryPolicy, so re-submitting would duplicate it
    /// - Max 3 attempts with exponential backoff + jitter: waits of 1s, then 2-3s
    ///
    /// The final error is returned unlogged; the caller reports it.
    pub async fn execute_workflow_with_retry(
        &self,
        handler: &SlackEventHandler,
        event_payload: serde_json::Value,
    ) -> Result<()> {
        let name = handler.data.as_ref().map_or("unknown", |d| d.name.as_str());
        let mut attempt = 0;
        loop {
            match self.execute_workflow(handler, event_payload.clone()).await {
                Ok(()) => {
                    if attempt > 0 {
                        tracing::info!(
                            "Handler execution succeeded after {} retries for handler '{}'",
                            attempt,
                            name
                        );
                    }
                    return Ok(());
                }
                Err(e)
                    if attempt + 1 >= Self::MAX_ATTEMPTS
                        || !shared::execution_failure::is_retriable_connection_failure(&e) =>
                {
                    return Err(e);
                }
                Err(e) => {
                    let base_delay = 2u64.pow(attempt);
                    let jitter = (base_delay as f64 * 0.5) as u64;
                    let delay = base_delay + (rand::random::<u64>() % (jitter + 1));
                    tracing::warn!(
                        "Handler execution failed (attempt {}/{}) for handler '{}', retrying in {}s: {:#}",
                        attempt + 1,
                        Self::MAX_ATTEMPTS,
                        name,
                        delay,
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Execute workflow (internal method, no retry)
    async fn execute_workflow(
        &self,
        handler: &SlackEventHandler,
        event_payload: serde_json::Value,
    ) -> Result<()> {
        let data = handler.data.as_ref().context("Handler data is required")?;

        // Get JobworkerpServer info from LocalConfigStore
        let jobworkerp_server = {
            let store = self
                .local_config_store
                .read()
                .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;

            let server_id = data
                .jobworkerp_server_id
                .as_ref()
                .context("jobworkerp_server_id is required")?;

            store
                .get_jobworkerp_server(server_id)
                .cloned()
                .with_context(|| format!("JobworkerpServer not found: id={}", server_id.value))?
        };

        let server_data = jobworkerp_server
            .data
            .as_ref()
            .context("JobworkerpServer data is required")?;

        // Build endpoint from host + port
        let protocol = if server_data.ssl_enabled {
            "https"
        } else {
            "http"
        };
        let endpoint = format!("{}://{}:{}", protocol, server_data.host, server_data.port);

        // Build workflow arguments: merge event_payload + handler.args
        let workflow_args = self.build_workflow_args(&event_payload, data.args.as_deref())?;

        // Capture the enqueue time before the (streaming) enqueue so triggered_at reflects when
        // the job was submitted, not when it completed.
        let triggered_at = chrono::Utc::now().timestamp();
        let server_id_value = data.jobworkerp_server_id.map(|id| id.value);
        let source_id = handler.id.as_ref().map(|id| id.value).unwrap_or_default();
        let trigger_context_json = event_payload.to_string();
        let endpoint_owned = endpoint.clone();
        let handler_name = data.name.clone();

        // Normalize the slack execution_target (+ deprecated workflow_url fallback) into the shared
        // ResolvedTarget. The args here carry the merged slack_event payload (slack-specific), so
        // only the target dispatch is shared; the args build stays on this path.
        let resolved_target = resolve_slack_target(data, &handler_name);

        // The streaming enqueue: returns the job_id immediately (when the runner supports
        // streaming) so the running job is trackable / cancellable while it executes.
        let enqueue = async {
            let target = resolved_target.ok_or_else(|| {
                anyhow::anyhow!(
                    "No execution target specified for handler '{}'",
                    handler_name
                )
            })?;
            shared::enqueue_by_target(&shared::ExecutionPlan {
                endpoint: endpoint_owned.clone(),
                target,
                args: Some(workflow_args.clone()),
                job_timeout_sec: None,
            })
            .await
        };

        // Record a pending ExecutionRef before enqueue (so the running job is visible), fill in
        // the job_id mid-flight, and the terminal result_status on completion. When no
        // jobworkerp_server_id is configured we cannot create a ref, so just run the enqueue.
        let outcome = match server_id_value {
            Some(server_id) => {
                let pending = ExecutionRef {
                    source_type: ExecutionSourceType::SlackEventHandler as i32,
                    source_id,
                    source_name: data.name.clone(),
                    jobworkerp_server_id: Some(JobworkerpServerId { value: server_id }),
                    triggered_at,
                    trigger_context_json: Some(trigger_context_json),
                    created_at: triggered_at,
                    ..Default::default()
                };
                shared::record_pending_then_update(&self.execution_ref_recorder, pending, enqueue)
                    .await
            }
            // No server_id to attach a ref to: run the enqueue and await its terminal outcome,
            // surfacing a setup error directly as the terminal error.
            None => match enqueue.await {
                Ok(enqueued) => enqueued.terminal.await,
                Err(e) => Err(e),
            },
        };

        match outcome {
            Ok(outcome) if outcome.success => {
                tracing::info!("Execution completed for handler '{}'", data.name);
                Ok(())
            }
            Ok(outcome) => {
                // Propagate the failure instead of masking it as Ok. It is not a connection
                // failure, so execute_workflow_with_retry does not re-submit the job: jobworkerp
                // already applied the worker's RetryPolicy before this terminal state.
                Err(anyhow::anyhow!(
                    "Job for handler '{}' reached a failed terminal state: {}",
                    data.name,
                    outcome.output
                ))
            }
            Err(e) => Err(e.context(format!("Failed to execute for handler '{}'", data.name))),
        }
    }

    /// Build workflow arguments by merging event payload and handler args
    fn build_workflow_args(
        &self,
        event_payload: &serde_json::Value,
        handler_args: Option<&str>,
    ) -> Result<String> {
        let mut merged = json!({
            "slack_event": event_payload
        });

        // Merge handler.args if present
        if let Some(args_str) = handler_args {
            if !args_str.is_empty() {
                let handler_args_json: serde_json::Value = serde_json::from_str(args_str)
                    .with_context(|| {
                        format!("Failed to parse handler.args as JSON: {}", args_str)
                    })?;

                if let serde_json::Value::Object(handler_map) = handler_args_json {
                    if let serde_json::Value::Object(ref mut merged_map) = merged {
                        for (key, value) in handler_map {
                            merged_map.insert(key, value);
                        }
                    }
                }
            }
        }

        serde_json::to_string(&merged).context("Failed to serialize merged arguments")
    }
}

/// Normalize a slack handler's `execution_target` (or the deprecated `workflow_url` fallback) into
/// the shared [`shared::ResolvedTarget`]. Returns `None` when neither is configured. Logs the chosen
/// target to preserve the previous per-branch tracing.
fn resolve_slack_target(
    data: &proto::jobworkerp_conductor::data::SlackEventHandlerData,
    handler_name: &str,
) -> Option<shared::ResolvedTarget> {
    match &data.execution_target {
        Some(ExecutionTarget::Worker(w)) => {
            tracing::info!(
                "Executing worker by name={} for handler '{}': using={:?}",
                w.worker_name,
                handler_name,
                w.using
            );
            Some(shared::ResolvedTarget::Worker {
                worker_name: w.worker_name.clone(),
                using: w.using.clone(),
            })
        }
        Some(ExecutionTarget::Workflow(wf)) => {
            tracing::info!(
                "Executing workflow for handler '{}': url={}, channel={:?}",
                handler_name,
                wf.workflow_url,
                wf.channel
            );
            Some(shared::ResolvedTarget::Workflow {
                workflow_url: wf.workflow_url.clone(),
                channel: wf.channel.clone(),
            })
        }
        None if !data.workflow_url.is_empty() => {
            let channel = (!data.channel.is_empty()).then(|| data.channel.clone());
            tracing::info!(
                "Executing workflow for handler '{}': url={}, channel={:?} (legacy fallback)",
                handler_name,
                data.workflow_url,
                channel
            );
            Some(shared::ResolvedTarget::Workflow {
                workflow_url: data.workflow_url.clone(),
                channel,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use proto::jobworkerp_conductor::data::{
        ExecutionRefId, JobworkerpServer, JobworkerpServerData, JobworkerpServerId,
        SlackEventHandlerData, SlackEventHandlerId,
    };
    use shared::execution_ref_recorder::ExecutionRefRecorder;
    use shared::LocalConfigStore;
    use std::sync::{Arc, Mutex, RwLock};

    /// Captures the pending ExecutionRefs created at enqueue time and the subsequent updates so
    /// tests can assert what was persisted. This is a real implementation of the recorder trait
    /// (an in-process boundary), not a mock of an external dependency.
    #[derive(Default)]
    struct CapturingRecorder {
        recorded: Mutex<Vec<ExecutionRef>>,
        enqueue_errors: Mutex<Vec<(i64, String)>>,
    }

    #[async_trait]
    impl ExecutionRefRecorder for CapturingRecorder {
        async fn record_execution_ref(
            &self,
            execution_ref: ExecutionRef,
        ) -> Result<ExecutionRefId> {
            self.recorded.lock().unwrap().push(execution_ref);
            Ok(ExecutionRefId { value: 1 })
        }
        async fn update_job_id(&self, _id: &ExecutionRefId, _job_id: i64) -> Result<()> {
            Ok(())
        }
        async fn update_result(
            &self,
            _id: &ExecutionRefId,
            _job_id: Option<i64>,
            _result_status: i32,
        ) -> Result<()> {
            Ok(())
        }
        async fn update_enqueue_error(&self, id: &ExecutionRefId, error: &str) -> Result<()> {
            self.enqueue_errors
                .lock()
                .unwrap()
                .push((id.value, error.to_string()));
            Ok(())
        }
    }

    fn handler_with_unreachable_server(id: i64) -> SlackEventHandler {
        SlackEventHandler {
            id: Some(SlackEventHandlerId { value: id }),
            data: Some(SlackEventHandlerData {
                name: format!("test_handler_{}", id),
                workflow_url: "http://example.invalid/workflow.yaml".to_string(),
                jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
                channel: String::new(),
                ..Default::default()
            }),
        }
    }

    fn config_store_with_unreachable_server() -> SharedLocalConfigStore {
        let mut store = LocalConfigStore::default();
        store
            .upsert_jobworkerp_server(JobworkerpServer {
                id: Some(JobworkerpServerId { value: 1 }),
                data: Some(JobworkerpServerData {
                    name: "unreachable".to_string(),
                    // Loopback + a closed port: connection is refused immediately,
                    // so enqueue fails fast (no job_id) without a long network timeout.
                    host: "127.0.0.1".to_string(),
                    port: "1".to_string(),
                    ssl_enabled: false,
                    enabled: true,
                    ..Default::default()
                }),
            })
            .unwrap();
        Arc::new(RwLock::new(store))
    }

    fn create_test_handler(id: i64, workflow_url: &str, args: Option<String>) -> SlackEventHandler {
        SlackEventHandler {
            id: Some(SlackEventHandlerId { value: id }),
            data: Some(SlackEventHandlerData {
                name: format!("test_handler_{}", id),
                workflow_url: workflow_url.to_string(),
                jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
                channel: String::new(),
                args,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_build_workflow_args_event_only() {
        let store = LocalConfigStore::default();
        let local_config_store = Arc::new(RwLock::new(store));
        let executor =
            HandlerExecutor::new(local_config_store, shared::noop_execution_ref_recorder());

        let event_payload = json!({
            "channel": "C123",
            "user": "U456",
            "text": "test message"
        });

        let args = executor.build_workflow_args(&event_payload, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&args).unwrap();

        assert_eq!(parsed["slack_event"]["channel"], "C123");
        assert_eq!(parsed["slack_event"]["user"], "U456");
    }

    #[test]
    fn test_build_workflow_args_with_merge() {
        let store = LocalConfigStore::default();
        let local_config_store = Arc::new(RwLock::new(store));
        let executor =
            HandlerExecutor::new(local_config_store, shared::noop_execution_ref_recorder());

        let event_payload = json!({
            "channel": "C123"
        });

        let handler_args = r#"{"custom_key": "custom_value", "timeout": 300}"#;

        let args = executor
            .build_workflow_args(&event_payload, Some(handler_args))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&args).unwrap();

        assert_eq!(parsed["slack_event"]["channel"], "C123");
        assert_eq!(parsed["custom_key"], "custom_value");
        assert_eq!(parsed["timeout"], 300);
    }

    #[test]
    fn test_build_workflow_args_invalid_json() {
        let store = LocalConfigStore::default();
        let local_config_store = Arc::new(RwLock::new(store));
        let executor =
            HandlerExecutor::new(local_config_store, shared::noop_execution_ref_recorder());

        let event_payload = json!({});
        let invalid_args = "not a json";

        let result = executor.build_workflow_args(&event_payload, Some(invalid_args));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("parse"));
    }

    // An enqueue failure (no job_id assigned, e.g. unreachable jobworkerp) must be recorded
    // with enqueue_error and propagated as Err so retry/EnqueueFailed handling can kick in.
    #[tokio::test]
    async fn execute_workflow_records_enqueue_error_and_returns_err_on_enqueue_failure() {
        let recorder = Arc::new(CapturingRecorder::default());
        let executor =
            HandlerExecutor::new(config_store_with_unreachable_server(), recorder.clone());

        let handler = handler_with_unreachable_server(42);
        let result = executor
            .execute_workflow(&handler, json!({"text": "hi"}))
            .await;

        assert!(result.is_err(), "enqueue failure must propagate as Err");

        // A pending ExecutionRef is created at enqueue time (job_id/result_status unset)...
        let recorded = recorder.recorded.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "exactly one pending execution_ref must be recorded"
        );
        let ref_ = &recorded[0];
        assert_eq!(ref_.source_id, 42);
        assert!(
            ref_.job_id.is_none(),
            "pending ref carries no job_id before enqueue"
        );
        // ...and the enqueue failure is recorded via update_enqueue_error so the status resolves
        // to EnqueueFailed.
        let errors = recorder.enqueue_errors.lock().unwrap();
        assert_eq!(errors.len(), 1, "enqueue_error must be recorded once");
        assert_eq!(errors[0].0, 1, "update targets the created ref id");
    }

    // A connection failure means no job was created, so it is retried up to MAX_ATTEMPTS and the
    // final Err is returned.
    #[tokio::test]
    async fn execute_workflow_with_retry_retries_connection_failure() {
        let recorder = Arc::new(CapturingRecorder::default());
        let executor =
            HandlerExecutor::new(config_store_with_unreachable_server(), recorder.clone());

        let handler = handler_with_unreachable_server(7);
        let result = executor
            .execute_workflow_with_retry(&handler, json!({"text": "hi"}))
            .await;

        assert!(
            result.is_err(),
            "exhausted retries on enqueue failure must surface as Err"
        );
        // Each attempt records a failed execution_ref.
        assert_eq!(
            recorder.recorded.lock().unwrap().len(),
            HandlerExecutor::MAX_ATTEMPTS as usize,
            "every attempt must be made and recorded"
        );
    }

    // Anything but a connection failure (here: no execution target configured) is not retried:
    // re-submitting cannot fix it, and job failures are retried by jobworkerp itself.
    #[tokio::test]
    async fn execute_workflow_with_retry_does_not_retry_non_connection_failure() {
        let recorder = Arc::new(CapturingRecorder::default());
        let executor =
            HandlerExecutor::new(config_store_with_unreachable_server(), recorder.clone());

        let handler = create_test_handler(8, "", None);
        let result = executor
            .execute_workflow_with_retry(&handler, json!({"text": "hi"}))
            .await;

        let err = result.expect_err("missing execution target must fail");
        assert!(
            format!("{err:#}").contains("No execution target"),
            "{err:#}"
        );
        assert_eq!(
            recorder.recorded.lock().unwrap().len(),
            1,
            "a non-connection failure must be attempted only once"
        );
    }
}
