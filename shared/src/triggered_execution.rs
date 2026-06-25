//! Shared execution-target dispatch for the three trigger paths (cron, slack, manual).
//!
//! The `execution_target` branch + streaming-first enqueue used to be duplicated in
//! `scheduler_manager.rs` and `handler_executor.rs`. [`enqueue_by_target`] is the single shared
//! piece; the surrounding pending-creation / terminal-wait control flow stays per-path because the
//! automatic paths (cron/slack) block until completion via [`crate::record_pending_then_update`],
//! while the manual trigger must return immediately ([`spawn_and_record`]).

use crate::execution_ref_recorder::{EnqueuedJob, SharedExecutionRefRecorder};
use crate::workflow_executor;
use anyhow::Result;
use jobworkerp_client::jobworkerp::data::JobId;
use proto::jobworkerp_conductor::data::{cron_scheduler_data::ExecutionTarget, ExecutionRefId};

/// A resolved execution target, normalized away from the per-source `oneof execution_target`
/// (`cron_scheduler_data::ExecutionTarget` / `slack_event_handler_data::ExecutionTarget`) plus the
/// deprecated `workflow_url` fallback, so this layer depends on neither proto variant.
#[derive(Debug, Clone)]
pub enum ResolvedTarget {
    Worker {
        worker_name: String,
        using: Option<String>,
    },
    Workflow {
        workflow_url: String,
        channel: Option<String>,
    },
}

impl ResolvedTarget {
    /// Normalize a cron `execution_target` (or the deprecated `workflow_url` fallback) into a
    /// [`ResolvedTarget`]. Returns `None` when neither a target nor a fallback URL is configured.
    /// Shared by the cron scheduler and the manual-trigger source resolver so the proto mapping
    /// lives in one place. An empty deprecated channel collapses to `None`.
    pub fn from_cron_target(
        execution_target: &Option<ExecutionTarget>,
        workflow_url_fallback: &str,
        channel_fallback: Option<&str>,
    ) -> Option<Self> {
        match execution_target {
            Some(ExecutionTarget::Worker(w)) => Some(ResolvedTarget::Worker {
                worker_name: w.worker_name.clone(),
                using: w.r#using.clone(),
            }),
            Some(ExecutionTarget::Workflow(wf)) => Some(ResolvedTarget::Workflow {
                workflow_url: wf.workflow_url.clone(),
                channel: wf.channel.clone(),
            }),
            None if !workflow_url_fallback.is_empty() => Some(ResolvedTarget::Workflow {
                workflow_url: workflow_url_fallback.to_string(),
                channel: channel_fallback
                    .filter(|c| !c.is_empty())
                    .map(str::to_string),
            }),
            _ => None,
        }
    }
}

/// Everything [`enqueue_by_target`] needs: where to run, what to run, and with which args.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub endpoint: String,
    pub target: ResolvedTarget,
    pub args: Option<String>,
}

/// Dispatch the resolved target to the matching streaming-first enqueue. Returns the immediate
/// `job_id` (when the runner supports streaming) plus the terminal-outcome future, wrapped in
/// [`EnqueuedJob`]. Shared by cron / slack / manual so the branch lives in exactly one place.
pub async fn enqueue_by_target(plan: &ExecutionPlan) -> Result<EnqueuedJob> {
    match &plan.target {
        ResolvedTarget::Worker { worker_name, using } => {
            workflow_executor::execute_worker_by_name_stream_first(
                worker_name,
                &plan.endpoint,
                plan.args.as_deref(),
                using.as_deref(),
            )
            .await
        }
        ResolvedTarget::Workflow {
            workflow_url,
            channel,
        } => {
            workflow_executor::execute_workflow_stream_first(
                workflow_url,
                &plan.endpoint,
                plan.args.as_deref(),
                channel.as_deref(),
            )
            .await
        }
    }
}

/// Manual-trigger record helper: enqueue `plan`, record the assigned `job_id` mid-flight when it is
/// known immediately, and detach the terminal-outcome monitoring so the caller can respond at once.
///
/// The caller must create the pending `ExecutionRef` first and pass its `id` (a manual trigger only
/// makes sense once the cancellable id exists). Differs from
/// [`crate::record_pending_then_update`] in two ways required by the RPC: it does not block on the
/// terminal future, and an enqueue-setup failure surfaces as `Err` (recorded as `enqueue_error`)
/// rather than being swallowed.
///
/// Return value mirrors the streaming client's `job_id`:
/// - `Ok(Some(job_id))` — streaming runner; `job_id` was recorded, the execution is cancellable.
/// - `Ok(None)` — Direct-fallback runner; `job_id` is only known once the (spawned) terminal future
///   resolves, so the caller reports `PENDING` / not-yet-cancellable.
/// - `Err(e)` — enqueue failed before a job was created; `enqueue_error` is recorded.
pub async fn spawn_and_record(
    recorder: SharedExecutionRefRecorder,
    id: ExecutionRefId,
    plan: ExecutionPlan,
) -> Result<Option<JobId>> {
    let EnqueuedJob { job_id, terminal } = match enqueue_by_target(&plan).await {
        Ok(enqueued) => enqueued,
        Err(e) => {
            // Enqueue setup failed before a job_id existed: record it on the already-created ref so
            // the status API reports ENQUEUE_FAILED, then propagate.
            if let Err(rec_err) = recorder.update_enqueue_error(&id, &e.to_string()).await {
                tracing::warn!("manual trigger: failed to record enqueue_error: {rec_err}");
            }
            return Err(e);
        }
    };

    // Streaming runner: the job_id is known now, so record it for live tracking / cancellation.
    if let Some(jid) = job_id.as_ref() {
        if let Err(e) = recorder.update_job_id(&id, jid.value).await {
            tracing::warn!("manual trigger: failed to record mid-flight job_id: {e}");
        }
    }

    // Detach terminal monitoring so the RPC returns immediately. On completion record the terminal
    // result (the recorder's update_result keeps the Cancelled guard); on a terminal error record
    // it as enqueue_error. For the Direct fallback the job_id only becomes available here.
    tokio::spawn(async move {
        match terminal.await {
            Ok(outcome) => {
                if let Err(e) = recorder
                    .update_result(&id, outcome.job_id.map(|j| j.value), outcome.status as i32)
                    .await
                {
                    tracing::warn!("manual trigger: failed to record terminal result: {e}");
                }
            }
            Err(e) => {
                if let Err(rec_err) = recorder.update_enqueue_error(&id, &e.to_string()).await {
                    tracing::warn!("manual trigger: failed to record terminal error: {rec_err}");
                }
            }
        }
    });

    Ok(job_id)
}

#[cfg(test)]
mod tests {
    use super::ResolvedTarget;
    use proto::jobworkerp_conductor::data::{
        cron_scheduler_data::ExecutionTarget, WorkerExecution, WorkflowExecution,
    };

    #[test]
    fn from_cron_target_resolves_worker() {
        let t = ResolvedTarget::from_cron_target(
            &Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: "w".to_string(),
                r#using: Some("run".to_string()),
            })),
            "",
            None,
        );
        assert!(matches!(
            t,
            Some(ResolvedTarget::Worker { worker_name, using })
                if worker_name == "w" && using.as_deref() == Some("run")
        ));
    }

    #[test]
    fn from_cron_target_resolves_workflow() {
        let t = ResolvedTarget::from_cron_target(
            &Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "http://wf".to_string(),
                channel: Some("c".to_string()),
            })),
            "",
            None,
        );
        assert!(matches!(
            t,
            Some(ResolvedTarget::Workflow { workflow_url, channel })
                if workflow_url == "http://wf" && channel.as_deref() == Some("c")
        ));
    }

    // The deprecated workflow_url is used only when no execution_target is set.
    #[test]
    fn from_cron_target_falls_back_to_deprecated_url() {
        let t = ResolvedTarget::from_cron_target(&None, "http://legacy", Some("ch"));
        assert!(matches!(
            t,
            Some(ResolvedTarget::Workflow { workflow_url, channel })
                if workflow_url == "http://legacy" && channel.as_deref() == Some("ch")
        ));
    }

    // An empty deprecated channel collapses to None.
    #[test]
    fn from_cron_target_empty_fallback_channel_is_none() {
        let t = ResolvedTarget::from_cron_target(&None, "http://legacy", Some(""));
        assert!(matches!(
            t,
            Some(ResolvedTarget::Workflow { channel, .. }) if channel.is_none()
        ));
    }

    #[test]
    fn from_cron_target_no_target_and_no_fallback_is_none() {
        assert!(ResolvedTarget::from_cron_target(&None, "", None).is_none());
    }
}
