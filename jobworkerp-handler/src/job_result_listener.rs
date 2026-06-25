use crate::settings::JobResultListenerSetting;
use anyhow::{Context, Result};
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_client::client::UseJobworkerpClient;
use jobworkerp_client::jobworkerp;
use jobworkerp_client::jobworkerp::data::JobResult;
use jobworkerp_client::jobworkerp::data::ResultStatus;
use jobworkerp_client::jobworkerp::service::ListenByWorkerRequest;
use jobworkerp_client::proto::JobworkerpProto;
use proto::jobworkerp_conductor::data::{ExecutionRef, ExecutionSourceType, JobworkerpServerId};
use std::ops::Deref;
use std::sync::Arc;
use std::vec;
use tokio::select;
use tokio::task::JoinSet;

#[derive(Clone)]
pub struct JobworkerpResultListener {
    workflow_list: Vec<JobResultListenerSetting>,
}

impl JobworkerpResultListener {
    pub async fn new(listeners: Vec<JobResultListenerSetting>) -> Result<Self> {
        Ok(Self {
            workflow_list: listeners,
        })
    }
    pub async fn listen_all(&self) -> Result<()> {
        // spawn all listen tasks for each worker in workflow_map in parallel
        let mut tasks = vec![];
        let mut set = JoinSet::new();

        for workflow_setting in self.workflow_list.iter().cloned() {
            tracing::info!(
                "worker_name: {}, workflow_settings: {:#?}, listen_jobworkerp: {}, process_jobworkerp: {}",
                &workflow_setting.name,
                &workflow_setting,
                &workflow_setting.listen_jobworkerp.address(),
                &workflow_setting.process_jobworkerp.address(),
            );
            // spawn listen tasks
            let task = set.spawn(async move { Self::listen(workflow_setting).await });
            tasks.push(task);
        }
        let results = set.join_all().await;
        for task in results.into_iter() {
            task?;
        }
        Ok(())
    }
    pub async fn listen(workflow_setting: JobResultListenerSetting) -> Result<()> {
        let listen_worker_name = Arc::new(workflow_setting.listen_worker_name.clone());
        let listen_workflow_settings = Arc::new(workflow_setting);
        loop {
            let job_result_client_wrapper =
                listen_workflow_settings.as_ref().listen_jobworkerp.clone();
            // connect to job result stream
            let mut stream = match job_result_client_wrapper
                .jobworkerp_client
                .job_result_client()
                .await
                .listen_by_worker(ListenByWorkerRequest {
                    worker: Some(
                        jobworkerp::service::listen_by_worker_request::Worker::WorkerName(
                            listen_worker_name.deref().clone(),
                        ),
                    ),
                })
                .await
            {
                Ok(stream) => stream.into_inner(),
                Err(e) => {
                    tracing::error!(
                        "ListenStreamByWorkerRequest error: {:?}, server: {}",
                        e,
                        &job_result_client_wrapper.jobworkerp_client().address
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    continue;
                }
            };

            // watch job result stream until disconnected
            loop {
                select! {
                    res = stream.message() => {
                    match res {
                        Ok(Some(job_result)) => {
                            // for move
                            let listen_workflow_settings = listen_workflow_settings.clone();
                            tokio::spawn(async move {
                                match Self::handle_job_result(
                                    listen_workflow_settings, &job_result
                                ).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        tracing::error!("handle_job_result error: {:?}", e);
                                    }
                                }
                            });
                        }
                        Ok(None) => break,
                        Err(e) => {
                            tracing::error!("Stream error: {:?}", e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                            break;
                        }
                    }
                    }
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("Received Ctrl-C, breaking the loop");
                        return Ok(());
                    }
                }
            }
            tracing::info!("Stream disconnected, retrying in 1 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    async fn handle_job_result(
        listen_workflow_settings: Arc<JobResultListenerSetting>,
        job_result: &JobResult,
    ) -> Result<()> {
        if let JobResult {
            id: Some(rid),
            data: Some(result_data),
            metadata: _,
        } = job_result
        {
            if result_data.status() != ResultStatus::Success {
                let err_mes = String::from_utf8_lossy(
                    result_data
                        .output
                        .as_ref()
                        .map(|out| &out.items)
                        .unwrap_or(&vec![]),
                )
                .to_string();
                tracing::info!(
                    "JobResult status is not success: {}, message: {:#?}",
                    &rid.value,
                    &err_mes
                );
            } else {
                tracing::debug!("JobResult success: job_result id={:#?}", &rid.value);
                let output_json = JobworkerpProto::resolve_result_output_to_json(
                    &listen_workflow_settings.listen_jobworkerp.jobworkerp_client,
                    listen_workflow_settings.listen_worker_name.as_str(),
                    result_data,
                    None, // using: for job result listener, we don't specify the method (auto-selected)
                )
                .await.inspect_err(|e| {
                        // fallback to raw output
                        let err = String::from_utf8_lossy(
                            result_data
                                .output
                                .as_ref()
                                .map(|out| out.items.as_ref())
                                .unwrap_or(&[]),
                        )
                        .to_string();
                        tracing::error!(
                            "resolve_result_output_to_string error (decode as string byte): {:?}, result candidate: {}",
                            e, &err
                        );
                })?;
                tracing::debug!("JobResult output: {:#?}", &output_json);
                // skip empty output // TODO use workflow input validation)
                if output_json.as_str().is_some_and(|s| s.is_empty()) {
                    tracing::debug!("JobResult output is empty. skip");
                } else {
                    // Capture enqueue time before enqueue so triggered_at reflects submission.
                    let triggered_at = chrono::Utc::now().timestamp();
                    let enqueue = Self::enqueue_workflow_stream_first(
                        listen_workflow_settings.process_jobworkerp.clone(),
                        listen_workflow_settings.clone(),
                        output_json,
                    );

                    // Record a pending ExecutionRef before enqueue (so the running process job is
                    // visible), fill in job_id mid-flight and result_status on completion. When no
                    // process_jobworkerp_server_id is configured we cannot create a ref, so just run.
                    let process_result = match listen_workflow_settings.process_jobworkerp_server_id
                    {
                        Some(process_server_id) => {
                            let trigger_context_json = serde_json::json!({
                                "listen_job_result_id": rid.value,
                                "listen_job_id": result_data.job_id.as_ref().map(|id| id.value),
                            })
                            .to_string();
                            let pending = Self::build_pending_execution_ref(
                                listen_workflow_settings.handler_id,
                                &listen_workflow_settings.name,
                                process_server_id,
                                trigger_context_json,
                                triggered_at,
                            );
                            shared::record_pending_then_update(
                                &listen_workflow_settings.execution_ref_recorder,
                                pending,
                                enqueue,
                            )
                            .await
                        }
                        // No server_id to attach a ref to: run the enqueue and await its terminal
                        // outcome, surfacing a setup error directly as the terminal error.
                        None => match enqueue.await {
                            Ok(enqueued) => enqueued.terminal.await,
                            Err(e) => Err(e),
                        },
                    };

                    match process_result {
                        Ok(outcome) => {
                            if outcome.success {
                                tracing::debug!("Result id: {:#?}", &rid.value);
                            } else {
                                tracing::warn!(
                                    "WorkerResultHandler workflow failed (job recorded): result id {}",
                                    &rid.value
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "handle_job_result error: {:?}, result id: {}",
                                e,
                                &rid.value
                            );
                        }
                    }
                }
            };
        } else {
            tracing::warn!("JobResult data is empty: {:#?}", &job_result);
        }
        Ok(())
    }
    /// Enqueue the process job for a listened result via the streaming path, returning the
    /// immediate job_id (when supported) plus the terminal-outcome future, so the running process
    /// job is trackable / cancellable while it executes. Reuses the already-connected
    /// `process_jobworkerp` client to avoid reconnection.
    pub async fn enqueue_workflow_stream_first(
        jobworkerp_client_wrapper: Arc<JobworkerpClientWrapper>,
        workflow_settings: Arc<JobResultListenerSetting>,
        input: serde_json::Value,
    ) -> Result<shared::execution_ref_recorder::EnqueuedJob> {
        // Combine job result with handler args
        let combined_input =
            Self::combine_result_and_args(&input, workflow_settings.args.as_deref());

        let (job_id, terminal) = if let Some(wname) = workflow_settings
            .worker_name
            .as_ref()
            .filter(|w| !w.is_empty())
        {
            // Worker execution mode
            tracing::info!(
                "Executing worker by name={} for listener '{}' (using: {:?})",
                wname,
                workflow_settings.name,
                workflow_settings.using
            );
            let args_json: serde_json::Value =
                serde_json::from_str(&combined_input).with_context(|| {
                    format!(
                        "Failed to parse combined args as JSON for listener '{}'",
                        workflow_settings.name
                    )
                })?;
            jobworkerp_client_wrapper
                .execute_worker_by_name_stream_first(
                    wname,
                    args_json,
                    workflow_settings.using.as_deref(),
                )
                .await?
        } else {
            // Workflow URL execution mode (default)
            jobworkerp_client_wrapper
                .execute_workflow_stream_first(
                    &workflow_settings.workflow_url,
                    &combined_input,
                    workflow_settings.channel.as_deref(),
                )
                .await?
        };

        Ok(shared::execution_ref_recorder::EnqueuedJob::from_terminal(
            job_id, terminal,
        ))
    }

    /// Build the pending ExecutionRef for a triggered process job, recorded at enqueue time before
    /// the job_id is known. `record_pending_then_update` fills in the job_id mid-flight and the
    /// terminal result_status on completion (or the enqueue_error on enqueue failure), mirroring
    /// the Cron and Slack paths.
    fn build_pending_execution_ref(
        handler_id: Option<i64>,
        handler_name: &str,
        process_server_id: i64,
        trigger_context_json: String,
        now: i64,
    ) -> ExecutionRef {
        ExecutionRef {
            source_type: ExecutionSourceType::WorkerResultHandler as i32,
            source_id: handler_id.unwrap_or_default(),
            source_name: handler_name.to_string(),
            jobworkerp_server_id: Some(JobworkerpServerId {
                value: process_server_id,
            }),
            triggered_at: now,
            trigger_context_json: Some(trigger_context_json),
            created_at: now,
            ..Default::default()
        }
    }

    /// Combine job result output with handler-level args into a single JSON string
    fn combine_result_and_args(result: &serde_json::Value, args: Option<&str>) -> String {
        match args {
            Some(args_str) if !args_str.is_empty() => {
                let args_value = serde_json::from_str(args_str)
                    .unwrap_or_else(|_| serde_json::Value::String(args_str.to_string()));
                serde_json::json!({
                    "result": result,
                    "args": args_value
                })
                .to_string()
            }
            _ => serde_json::json!({ "result": result }).to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // The pending ExecutionRef recorded at enqueue time carries the source identity and trigger
    // context but no job_id / result_status / enqueue_error; those are filled in later by
    // record_pending_then_update (job_id mid-flight, result_status or enqueue_error on completion).
    #[test]
    fn build_pending_execution_ref_sets_source_and_context_only() {
        let ref_ = JobworkerpResultListener::build_pending_execution_ref(
            Some(7),
            "handler",
            3,
            r#"{"listen_job_result_id":1}"#.to_string(),
            100,
        );

        assert_eq!(
            ref_.source_type,
            ExecutionSourceType::WorkerResultHandler as i32
        );
        assert_eq!(ref_.source_id, 7);
        assert_eq!(ref_.source_name, "handler");
        assert_eq!(ref_.jobworkerp_server_id.map(|s| s.value), Some(3));
        assert_eq!(ref_.triggered_at, 100);
        assert_eq!(ref_.created_at, 100);
        assert_eq!(
            ref_.trigger_context_json.as_deref(),
            Some(r#"{"listen_job_result_id":1}"#)
        );
        // Pending: not yet known.
        assert!(ref_.job_id.is_none());
        assert!(ref_.result_status.is_none());
        assert!(ref_.enqueue_error.is_none());
    }

    // A missing handler_id defaults source_id to 0 (handler_id is optional in legacy settings).
    #[test]
    fn build_pending_execution_ref_defaults_missing_handler_id() {
        let ref_ = JobworkerpResultListener::build_pending_execution_ref(
            None,
            "handler",
            3,
            "{}".to_string(),
            100,
        );
        assert_eq!(ref_.source_id, 0);
    }

    #[test]
    fn test_combine_result_and_args_with_json_args() {
        let result = json!({
            "job_id": "12345",
            "status": "completed",
            "output": "処理完了"
        });
        let args = Some(r#"{"retry_limit": 5, "timeout_seconds": 300}"#);

        let combined = JobworkerpResultListener::combine_result_and_args(&result, args);
        let parsed: serde_json::Value = serde_json::from_str(&combined).unwrap();

        assert_eq!(parsed["result"], result);
        assert_eq!(parsed["args"]["retry_limit"], 5);
        assert_eq!(parsed["args"]["timeout_seconds"], 300);
    }

    #[test]
    fn test_combine_result_and_args_with_string_args() {
        let result = json!({"status": "success"});
        let args = Some("env=production,batch_size=100");

        let combined = JobworkerpResultListener::combine_result_and_args(&result, args);
        let parsed: serde_json::Value = serde_json::from_str(&combined).unwrap();

        assert_eq!(parsed["result"], result);
        assert_eq!(parsed["args"], "env=production,batch_size=100");
    }

    #[test]
    fn test_combine_result_and_args_with_no_args() {
        let result = json!({"job_id": "abc", "status": "failed"});
        let args = None;

        let combined = JobworkerpResultListener::combine_result_and_args(&result, args);
        let parsed: serde_json::Value = serde_json::from_str(&combined).unwrap();

        assert_eq!(parsed["result"], result);
        assert!(parsed["args"].is_null());
        assert_eq!(parsed.as_object().unwrap().len(), 1); // Only "result" field
    }

    #[test]
    fn test_combine_result_and_args_with_empty_args() {
        let result = json!({"data": "test"});
        let args = Some("");

        let combined = JobworkerpResultListener::combine_result_and_args(&result, args);
        let parsed: serde_json::Value = serde_json::from_str(&combined).unwrap();

        assert_eq!(parsed["result"], result);
        assert!(parsed["args"].is_null());
        assert_eq!(parsed.as_object().unwrap().len(), 1); // Only "result" field
    }

    #[test]
    fn test_combine_result_and_args_with_invalid_json_args() {
        let result = json!({"test": true});
        let args = Some("{invalid json");

        let combined = JobworkerpResultListener::combine_result_and_args(&result, args);
        let parsed: serde_json::Value = serde_json::from_str(&combined).unwrap();

        assert_eq!(parsed["result"], result);
        assert_eq!(parsed["args"], "{invalid json"); // Falls back to string
    }
}
