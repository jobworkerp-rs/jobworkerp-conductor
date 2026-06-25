use crate::workflow_executor::WorkflowExecutionOutcome;
use anyhow::Result;
use async_trait::async_trait;
use proto::jobworkerp_conductor::data::{ExecutionRef, ExecutionRefId};
use std::future::Future;
use std::sync::Arc;

#[async_trait]
pub trait ExecutionRefRecorder: Send + Sync {
    /// Record a (typically pending) execution_ref and return its id so the terminal outcome can
    /// be filled in later via the update methods.
    async fn record_execution_ref(&self, execution_ref: ExecutionRef) -> Result<ExecutionRefId>;
    /// Record the assigned job_id mid-flight (before the job reaches a terminal state), leaving
    /// result_status untouched so the status API reports the live processing status (not a
    /// premature terminal status) while the job runs.
    async fn update_job_id(&self, id: &ExecutionRefId, job_id: i64) -> Result<()>;
    /// Fill in the assigned job_id and observed terminal result_status of a pending execution_ref.
    async fn update_result(
        &self,
        id: &ExecutionRefId,
        job_id: Option<i64>,
        result_status: i32,
    ) -> Result<()>;
    /// Record the enqueue error of a pending execution_ref that never reached a terminal job.
    async fn update_enqueue_error(&self, id: &ExecutionRefId, error: &str) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoopExecutionRefRecorder;

#[async_trait]
impl ExecutionRefRecorder for NoopExecutionRefRecorder {
    async fn record_execution_ref(&self, _execution_ref: ExecutionRef) -> Result<ExecutionRefId> {
        Ok(ExecutionRefId { value: 0 })
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
    async fn update_enqueue_error(&self, _id: &ExecutionRefId, _error: &str) -> Result<()> {
        Ok(())
    }
}

pub type SharedExecutionRefRecorder = Arc<dyn ExecutionRefRecorder>;

pub fn noop_execution_ref_recorder() -> SharedExecutionRefRecorder {
    Arc::new(NoopExecutionRefRecorder)
}

/// Enqueue result split into the (optionally immediate) job_id and the future resolving to the
/// terminal outcome. Streaming-capable runners return `job_id` right after enqueue (before the
/// job finishes), enabling mid-flight tracking / cancellation; runners that fall back to the
/// blocking Direct path return `job_id = None` here and surface it only via the terminal outcome.
pub struct EnqueuedJob {
    pub job_id: Option<jobworkerp_client::jobworkerp::data::JobId>,
    pub terminal: std::pin::Pin<Box<dyn Future<Output = Result<WorkflowExecutionOutcome>> + Send>>,
}

impl EnqueuedJob {
    /// Build from a streaming-client terminal future whose outcome only needs `Into` conversion
    /// into `WorkflowExecutionOutcome`, boxing it for the type-erased `terminal` field.
    pub fn from_terminal<T, Fut>(
        job_id: Option<jobworkerp_client::jobworkerp::data::JobId>,
        terminal: Fut,
    ) -> Self
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Into<WorkflowExecutionOutcome>,
    {
        EnqueuedJob {
            job_id,
            terminal: Box::pin(async move { terminal.await.map(Into::into) }),
        }
    }

    /// Build an `EnqueuedJob` whose terminal future immediately yields `e`. Used to turn an
    /// enqueue-setup error (raised before the job could be submitted) into a terminal error so
    /// `record_pending_then_update` records it as the ExecutionRef's enqueue_error.
    pub fn enqueue_failure(e: anyhow::Error) -> Self {
        EnqueuedJob {
            job_id: None,
            terminal: Box::pin(async move { Err(e) }),
        }
    }
}

/// Record `pending` before enqueue, fill in `job_id` as soon as it is known (mid-flight, so the
/// running job is trackable / cancellable), then fill in the terminal `result_status` once the
/// job finishes.
///
/// This is the shared orchestration used by the cron / Slack / WorkerResultHandler paths so that
/// a job's ExecutionRef exists in the DB while the job is still running, with `triggered_at`
/// reflecting the enqueue time rather than the completion time. `pending` must carry
/// `job_id = None` and `result_status = None`. An `enqueue` that resolves to `Err` (setup failed
/// before the job could be submitted) is treated as a terminal enqueue error.
///
/// Recording is best-effort: if the initial create fails there is no id to update, so the updates
/// are skipped and the original outcome / error is still returned to the caller unchanged.
pub async fn record_pending_then_update<F>(
    recorder: &SharedExecutionRefRecorder,
    pending: ExecutionRef,
    enqueue: F,
) -> Result<WorkflowExecutionOutcome>
where
    F: Future<Output = Result<EnqueuedJob>>,
{
    let id = match recorder.record_execution_ref(pending).await {
        Ok(id) => Some(id),
        Err(e) => {
            tracing::warn!("Failed to record pending execution_ref: {}", e);
            None
        }
    };

    let EnqueuedJob { job_id, terminal } =
        enqueue.await.unwrap_or_else(EnqueuedJob::enqueue_failure);

    // Streaming runners provide the job_id here, before the terminal state, so the running job can
    // be tracked / cancelled; the Direct fallback leaves it None until the terminal update below.
    if let (Some(id), Some(jid)) = (id.as_ref(), job_id.as_ref()) {
        if let Err(e) = recorder.update_job_id(id, jid.value).await {
            tracing::warn!("Failed to record mid-flight job_id on execution_ref: {}", e);
        }
    }

    let result = terminal.await;

    if let Some(id) = id {
        match &result {
            Ok(outcome) => {
                if let Err(e) = recorder
                    .update_result(&id, outcome.job_id.map(|j| j.value), outcome.status as i32)
                    .await
                {
                    tracing::warn!("Failed to update execution_ref result: {}", e);
                }
            }
            Err(e) => {
                if let Err(update_err) = recorder.update_enqueue_error(&id, &e.to_string()).await {
                    tracing::warn!(
                        "Failed to update execution_ref enqueue_error: {}",
                        update_err
                    );
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use jobworkerp_client::jobworkerp::data::{JobId, ResultStatus};
    use std::sync::Mutex;

    #[derive(Debug)]
    enum Call {
        Record,
        UpdateJobId {
            id: i64,
            job_id: i64,
        },
        UpdateResult {
            id: i64,
            job_id: Option<i64>,
            result_status: i32,
        },
        UpdateEnqueueError {
            id: i64,
            error: String,
        },
    }

    /// Real recorder implementation (not a mock library) that records the calls it receives,
    /// so the orchestration order/arguments can be asserted with real dependencies.
    #[derive(Default)]
    struct RecordingRecorder {
        calls: Mutex<Vec<Call>>,
        next_id: i64,
    }

    #[async_trait]
    impl ExecutionRefRecorder for RecordingRecorder {
        async fn record_execution_ref(
            &self,
            _execution_ref: ExecutionRef,
        ) -> Result<ExecutionRefId> {
            self.calls.lock().unwrap().push(Call::Record);
            Ok(ExecutionRefId {
                value: self.next_id,
            })
        }
        async fn update_job_id(&self, id: &ExecutionRefId, job_id: i64) -> Result<()> {
            self.calls.lock().unwrap().push(Call::UpdateJobId {
                id: id.value,
                job_id,
            });
            Ok(())
        }
        async fn update_result(
            &self,
            id: &ExecutionRefId,
            job_id: Option<i64>,
            result_status: i32,
        ) -> Result<()> {
            self.calls.lock().unwrap().push(Call::UpdateResult {
                id: id.value,
                job_id,
                result_status,
            });
            Ok(())
        }
        async fn update_enqueue_error(&self, id: &ExecutionRefId, error: &str) -> Result<()> {
            self.calls.lock().unwrap().push(Call::UpdateEnqueueError {
                id: id.value,
                error: error.to_string(),
            });
            Ok(())
        }
    }

    fn pending() -> ExecutionRef {
        ExecutionRef {
            triggered_at: 100,
            created_at: 100,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn ok_outcome_records_then_updates_result() {
        let rec = Arc::new(RecordingRecorder {
            next_id: 7,
            ..Default::default()
        });
        let recorder: SharedExecutionRefRecorder = rec.clone();
        let outcome = WorkflowExecutionOutcome {
            job_id: Some(JobId { value: 42 }),
            success: true,
            status: ResultStatus::Success,
            output: serde_json::Value::Null,
        };
        // Streaming runner: job_id known immediately, terminal outcome resolves later.
        let result = record_pending_then_update(&recorder, pending(), async {
            Ok(EnqueuedJob {
                job_id: Some(JobId { value: 42 }),
                terminal: Box::pin(async { Ok(outcome) }),
            })
        })
        .await;
        assert!(result.is_ok());

        let calls = rec.calls.lock().unwrap();
        assert!(matches!(calls[0], Call::Record));
        // Mid-flight: job_id recorded before the terminal status.
        match &calls[1] {
            Call::UpdateJobId { id, job_id } => {
                assert_eq!(*id, 7);
                assert_eq!(*job_id, 42);
            }
            other => panic!("expected UpdateJobId, got {other:?}"),
        }
        match &calls[2] {
            Call::UpdateResult {
                id,
                job_id,
                result_status,
            } => {
                assert_eq!(*id, 7);
                assert_eq!(*job_id, Some(42));
                assert_eq!(*result_status, ResultStatus::Success as i32);
            }
            other => panic!("expected UpdateResult, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_immediate_job_id_skips_midflight_update() {
        let rec = Arc::new(RecordingRecorder {
            next_id: 8,
            ..Default::default()
        });
        let recorder: SharedExecutionRefRecorder = rec.clone();
        let outcome = WorkflowExecutionOutcome {
            job_id: Some(JobId { value: 55 }),
            success: false,
            status: ResultStatus::FatalError,
            output: serde_json::Value::Null,
        };
        // Direct-fallback path: no immediate job_id; it arrives only with the terminal outcome.
        let result = record_pending_then_update(&recorder, pending(), async {
            Ok(EnqueuedJob {
                job_id: None,
                terminal: Box::pin(async { Ok(outcome) }),
            })
        })
        .await;
        assert!(result.is_ok());

        let calls = rec.calls.lock().unwrap();
        assert!(matches!(calls[0], Call::Record));
        // No mid-flight UpdateJobId; goes straight to the terminal UpdateResult.
        match &calls[1] {
            Call::UpdateResult {
                job_id,
                result_status,
                ..
            } => {
                assert_eq!(*job_id, Some(55));
                assert_eq!(*result_status, ResultStatus::FatalError as i32);
            }
            other => panic!("expected UpdateResult, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn err_outcome_records_then_updates_enqueue_error() {
        let rec = Arc::new(RecordingRecorder {
            next_id: 9,
            ..Default::default()
        });
        let recorder: SharedExecutionRefRecorder = rec.clone();
        let result = record_pending_then_update(&recorder, pending(), async {
            Ok(EnqueuedJob {
                job_id: None,
                terminal: Box::pin(async { Err(anyhow::anyhow!("connection refused")) }),
            })
        })
        .await;
        assert!(result.is_err());

        let calls = rec.calls.lock().unwrap();
        assert!(matches!(calls[0], Call::Record));
        match &calls[1] {
            Call::UpdateEnqueueError { id, error } => {
                assert_eq!(*id, 9);
                assert!(error.contains("connection refused"));
            }
            other => panic!("expected UpdateEnqueueError, got {other:?}"),
        }
    }

    // An enqueue setup error (the Err arm of the enqueue future, before any job was submitted) is
    // recorded as an enqueue_error and propagated, just like a terminal Err outcome.
    #[tokio::test]
    async fn enqueue_setup_error_records_enqueue_error() {
        let rec = Arc::new(RecordingRecorder {
            next_id: 11,
            ..Default::default()
        });
        let recorder: SharedExecutionRefRecorder = rec.clone();
        let result = record_pending_then_update(&recorder, pending(), async {
            Err(anyhow::anyhow!("failed to connect"))
        })
        .await;
        assert!(result.is_err());

        let calls = rec.calls.lock().unwrap();
        assert!(matches!(calls[0], Call::Record));
        match &calls[1] {
            Call::UpdateEnqueueError { id, error } => {
                assert_eq!(*id, 11);
                assert!(error.contains("failed to connect"));
            }
            other => panic!("expected UpdateEnqueueError, got {other:?}"),
        }
    }
}
