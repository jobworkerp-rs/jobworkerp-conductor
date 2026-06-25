use crate::app::source_resolver::{DbExecutionRefRecorder, ExecutionSourceResolver};
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra::error::UiEventHandlerError;
use infra::infra::execution_ref::rdb::{
    ExecutionRefListFilter, ExecutionRefRepository, ExecutionRefRepositoryImpl,
    UseExecutionRefRepository,
};
use infra::infra::jobworkerp_server::rdb::{
    JobworkerpServerRepository, JobworkerpServerRepositoryImpl, UseJobworkerpServerRepository,
};
use infra_utils::infra::rdb::UseRdbPool;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_client::command::to_request;
use jobworkerp_client::jobworkerp::data::{JobId, JobProcessingStatus, ResultStatus};
use jobworkerp_client::jobworkerp::service::FindListByJobIdRequest;
use proto::jobworkerp_conductor::data::{
    ExecutionRef, ExecutionRefId, ExecutionRuntimeStatus, ExecutionSourceType,
    ExecutionStatusSource, ResolvedExecutionStatus,
};
use shared::{ExecutionPlan, ExecutionRefRecorder, SharedExecutionRefRecorder};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[async_trait]
pub trait ExecutionStatusApp:
    UseExecutionRefRepository + UseJobworkerpServerRepository + Send + Sync + Sized + 'static
{
    async fn create_execution_ref(&self, execution_ref: &ExecutionRef) -> Result<ExecutionRefId>;
    // The pending-then-update semantics live on `ExecutionRefRepository`; these are thin pass-throughs.
    async fn update_execution_ref_job_id(&self, id: &ExecutionRefId, job_id: i64) -> Result<()>;
    async fn update_execution_ref_result(
        &self,
        id: &ExecutionRefId,
        job_id: Option<i64>,
        result_status: i32,
    ) -> Result<()>;
    async fn update_execution_ref_enqueue_error(
        &self,
        id: &ExecutionRefId,
        error: &str,
    ) -> Result<()>;
    async fn find_execution_ref(&self, id: &ExecutionRefId) -> Result<Option<ExecutionRef>>;
    async fn find_latest_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
    ) -> Result<Option<ExecutionRef>>;
    async fn find_list_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<ExecutionRef>>;
    async fn find_runtime_status(
        &self,
        id: &ExecutionRefId,
    ) -> Result<Option<ExecutionRuntimeStatus>>;
    async fn find_latest_runtime_status_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
    ) -> Result<Option<ExecutionRuntimeStatus>>;
    async fn cancel_execution(&self, id: &ExecutionRefId) -> Result<bool>;

    /// Delete a single execution_ref, refusing to delete one that is not in a terminal state.
    /// Returns `DeleteResult` so the gRPC layer can distinguish deleted / missing / not-terminal
    /// without string matching.
    async fn delete_execution_ref(&self, id: &ExecutionRefId) -> Result<DeleteResult>;

    /// Delete the execution_refs of a source. With `include_active=false` only terminal refs are
    /// removed (active or status-indeterminate ones are kept); with `include_active=true` status
    /// resolution is bypassed and every ref of the source is removed. Returns the number deleted.
    async fn delete_execution_refs_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
        include_active: bool,
    ) -> Result<u64>;

    /// Cross-source listing filtered/paged in the database (no per-source fan-out).
    async fn find_list(
        &self,
        filter: ExecutionRefListFilter,
        limit: Option<i32>,
        offset: Option<i64>,
    ) -> Result<Vec<ExecutionRef>>;

    /// Count of refs matching `filter` (for pager totals).
    async fn count_list(&self, filter: ExecutionRefListFilter) -> Result<i64>;

    /// Manually trigger one execution of a configured source (Cron only in MVP). Creates the
    /// pending ExecutionRef first (failure aborts before enqueue), enqueues, and returns as soon as
    /// the job_id is known; terminal monitoring runs in a detached task. `args_json` replaces the
    /// configured args when present.
    async fn trigger_execution(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
        args_json: Option<String>,
    ) -> Result<TriggerOutcome>;

    /// Re-run the source an existing ExecutionRef points at, using the source's current config
    /// (args are NOT carried over from the original ref — it does not persist them). Cron only;
    /// other source types and a missing ref are rejected. A shortcut over `trigger_execution`.
    async fn re_execute(&self, id: &ExecutionRefId) -> Result<TriggerOutcome>;
}

/// Result of a manual trigger / re-execute: the created ref id plus the runtime status observed
/// right after enqueue (usually PENDING/RUNNING).
pub struct TriggerOutcome {
    pub execution_ref_id: Option<ExecutionRefId>,
    pub status: ExecutionRuntimeStatus,
}

/// Outcome of a single-ref delete. Separated from a bare `bool` so the gRPC layer can map the
/// "exists but not deletable" case to `FAILED_PRECONDITION` rather than a silent failure.
#[derive(Debug, PartialEq, Eq)]
pub enum DeleteResult {
    Deleted,
    NotFound,
    NotTerminal,
}

/// Build the gRPC endpoint string for a jobworkerp server from its data. Shared by `client_for`
/// (which then opens a client) and the manual-trigger source resolver (which only needs the string).
pub(crate) fn server_endpoint(
    data: &proto::jobworkerp_conductor::data::JobworkerpServerData,
) -> String {
    let protocol = if data.ssl_enabled { "https" } else { "http" };
    format!("{}://{}:{}", protocol, data.host, data.port)
}

/// A finished execution that may be physically deleted. Refs whose runtime status cannot be
/// confirmed terminal (active, or jobworkerp-unreachable `Unknown`/`Unavailable`) are protected:
/// deleting them would drop a still-trackable / cancellable execution from the ledger. Mirrors the
/// allowlist in plan §5.3.
fn is_terminal(resolved: ResolvedExecutionStatus) -> bool {
    matches!(
        resolved,
        ResolvedExecutionStatus::Succeeded
            | ResolvedExecutionStatus::Failed
            | ResolvedExecutionStatus::Cancelled
            | ResolvedExecutionStatus::EnqueueFailed
    )
}

pub struct ExecutionStatusAppImpl {
    execution_ref_repository: ExecutionRefRepositoryImpl,
    jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
    /// Resolves a (source_type, source_id) into the concrete enqueue plan for the manual trigger.
    /// One-directional (this app → cron app), so no dependency cycle.
    source_resolver: Arc<dyn ExecutionSourceResolver>,
    /// Lightweight DB-only recorder handed to the detached terminal task of `spawn_and_record`.
    recorder: SharedExecutionRefRecorder,
}

impl ExecutionStatusAppImpl {
    pub fn new(
        execution_ref_repository: ExecutionRefRepositoryImpl,
        jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
        source_resolver: Arc<dyn ExecutionSourceResolver>,
    ) -> Self {
        // The detached terminal monitoring only needs the execution_ref repository.
        let recorder: SharedExecutionRefRecorder = Arc::new(DbExecutionRefRecorder::new(
            execution_ref_repository.clone(),
        ));
        Self {
            execution_ref_repository,
            jobworkerp_server_repository,
            source_resolver,
            recorder,
        }
    }

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or_default()
    }

    /// Build a RUNNING runtime status sourced from the ExecutionRef itself, used right after a manual
    /// trigger when the live status resolves to nothing or the job_id is not yet assigned (Direct
    /// fallback): the job is enqueued and the conductor is awaiting its result. `detail` carries the
    /// not-yet-cancellable note when applicable. Mirrors the `resolve_status` interpretation of a
    /// job_id-less, error-less, result-less ref so a follow-up FindRuntimeStatus stays consistent.
    fn running_status_without_job_id(
        &self,
        id: &ExecutionRefId,
        detail: Option<String>,
    ) -> ExecutionRuntimeStatus {
        ExecutionRuntimeStatus {
            execution_ref: Some(ExecutionRef {
                id: Some(*id),
                ..Default::default()
            }),
            resolved_status: ResolvedExecutionStatus::Running as i32,
            status_source: ExecutionStatusSource::ExecutionRef as i32,
            observed_at: Self::now(),
            detail,
        }
    }

    async fn client_for(&self, execution_ref: &ExecutionRef) -> Result<JobworkerpClientWrapper> {
        let server_id = execution_ref
            .jobworkerp_server_id
            .as_ref()
            .context("execution_ref.jobworkerp_server_id is missing")?;
        let server = self
            .jobworkerp_server_repository()
            .find(server_id)
            .await?
            .context("jobworkerp server not found")?;
        let data = server.data.context("jobworkerp server data is missing")?;
        JobworkerpClientWrapper::new(&server_endpoint(&data), None).await
    }

    async fn resolve_status(&self, execution_ref: ExecutionRef) -> ExecutionRuntimeStatus {
        // Without a job_id there is nothing to query jobworkerp with, so the status is resolved from
        // the ref alone (see `resolve_jobidless_status` for the case breakdown).
        let Some(job_id_value) = execution_ref.job_id else {
            let (resolved_status, detail) = resolve_jobidless_status(
                execution_ref.enqueue_error.as_deref(),
                execution_ref.result_status,
            );
            return ExecutionRuntimeStatus {
                execution_ref: Some(execution_ref),
                resolved_status,
                status_source: ExecutionStatusSource::ExecutionRef as i32,
                observed_at: Self::now(),
                detail: Some(detail),
            };
        };

        let client = match self.client_for(&execution_ref).await {
            Ok(client) => client,
            Err(e) => {
                return ExecutionRuntimeStatus {
                    execution_ref: Some(execution_ref),
                    resolved_status: ResolvedExecutionStatus::Unavailable as i32,
                    status_source: ExecutionStatusSource::Unavailable as i32,
                    observed_at: Self::now(),
                    detail: Some(e.to_string()),
                };
            }
        };

        let job_id = JobId {
            value: job_id_value,
        };
        let metadata = HashMap::new();
        match client
            .jobworkerp_client
            .job_processing_status_client()
            .await
            .find(to_request(&metadata, job_id).expect("metadata should be valid"))
            .await
        {
            Ok(response) => {
                if let Some(status) = response.into_inner().status {
                    return ExecutionRuntimeStatus {
                        execution_ref: Some(execution_ref),
                        resolved_status: map_processing_status(status),
                        status_source: ExecutionStatusSource::JobProcessingStatus as i32,
                        observed_at: Self::now(),
                        detail: None,
                    };
                }
            }
            Err(e) => {
                return ExecutionRuntimeStatus {
                    execution_ref: Some(execution_ref),
                    resolved_status: ResolvedExecutionStatus::Unavailable as i32,
                    status_source: ExecutionStatusSource::Unavailable as i32,
                    observed_at: Self::now(),
                    detail: Some(e.to_string()),
                };
            }
        }

        let request = FindListByJobIdRequest {
            job_id: Some(JobId {
                value: job_id_value,
            }),
        };
        match client
            .jobworkerp_client
            .job_result_client()
            .await
            .find_list_by_job_id(to_request(&metadata, request).expect("metadata should be valid"))
            .await
        {
            Ok(response) => {
                let mut stream = response.into_inner();
                match stream.message().await {
                    Ok(Some(result)) => {
                        let status = result
                            .data
                            .as_ref()
                            .map(|d| d.status())
                            .unwrap_or(ResultStatus::OtherError);
                        ExecutionRuntimeStatus {
                            execution_ref: Some(execution_ref),
                            resolved_status: map_result_status(status),
                            status_source: ExecutionStatusSource::JobResult as i32,
                            observed_at: Self::now(),
                            detail: None,
                        }
                    }
                    Ok(None) => {
                        // No processing status and no stored JobResult. Prefer the terminal status
                        // captured at execution time: a job may legitimately leave no result
                        // (worker store_failure=false, or a cancelled PENDING job), so inferring
                        // Succeeded unconditionally would mask failures. Fall back to inference
                        // only for refs recorded before result_status was tracked.
                        let (resolved_status, detail) =
                            resolve_status_without_stored_result(execution_ref.result_status);
                        ExecutionRuntimeStatus {
                            execution_ref: Some(execution_ref),
                            resolved_status,
                            status_source: ExecutionStatusSource::ExecutionRef as i32,
                            observed_at: Self::now(),
                            detail: Some(detail),
                        }
                    }
                    Err(e) => ExecutionRuntimeStatus {
                        execution_ref: Some(execution_ref),
                        resolved_status: ResolvedExecutionStatus::Unavailable as i32,
                        status_source: ExecutionStatusSource::Unavailable as i32,
                        observed_at: Self::now(),
                        detail: Some(e.to_string()),
                    },
                }
            }
            Err(e) => ExecutionRuntimeStatus {
                execution_ref: Some(execution_ref),
                resolved_status: ResolvedExecutionStatus::Unavailable as i32,
                status_source: ExecutionStatusSource::Unavailable as i32,
                observed_at: Self::now(),
                detail: Some(e.to_string()),
            },
        }
    }
}

fn map_processing_status(status: i32) -> i32 {
    match JobProcessingStatus::try_from(status).unwrap_or(JobProcessingStatus::Unknown) {
        JobProcessingStatus::Pending => ResolvedExecutionStatus::Pending as i32,
        JobProcessingStatus::Running => ResolvedExecutionStatus::Running as i32,
        JobProcessingStatus::WaitResult => ResolvedExecutionStatus::WaitResult as i32,
        JobProcessingStatus::Cancelling => ResolvedExecutionStatus::Cancelling as i32,
        JobProcessingStatus::Unknown => ResolvedExecutionStatus::Unknown as i32,
    }
}

fn map_result_status(status: ResultStatus) -> i32 {
    match status {
        ResultStatus::Success => ResolvedExecutionStatus::Succeeded as i32,
        ResultStatus::Cancelled => ResolvedExecutionStatus::Cancelled as i32,
        _ => ResolvedExecutionStatus::Failed as i32,
    }
}

/// Fallback for a finished job that left no processing status, no stored result, AND no
/// recorded terminal status on the ExecutionRef (i.e. a legacy ref created before
/// `result_status` was tracked).
///
/// Workflow executions enqueue ephemeral workers with `store_success=false` /
/// `store_failure=true`, so for those a missing result implies success. This assumption does
/// NOT hold for pre-registered workers with `store_failure=false` or for cancelled PENDING
/// jobs; those cases are now disambiguated by the recorded `result_status`, leaving this
/// inference only for legacy refs.
fn infer_status_without_result() -> i32 {
    ResolvedExecutionStatus::Succeeded as i32
}

/// Resolve the status of a job that has no processing status and no stored JobResult, using the
/// terminal `result_status` recorded on the ExecutionRef when present. Returns the resolved
/// status plus a human-readable detail. Split out as a pure function so the success/failure
/// disambiguation can be unit-tested without a live jobworkerp server.
fn resolve_status_without_stored_result(recorded_result_status: Option<i32>) -> (i32, String) {
    match recorded_result_status {
        Some(rs) => (
            map_result_status(ResultStatus::try_from(rs).unwrap_or(ResultStatus::OtherError)),
            "no stored result; using terminal status recorded at execution time".to_string(),
        ),
        None => (
            infer_status_without_result(),
            "no processing status, stored result, or recorded terminal status; success inferred for legacy refs".to_string(),
        ),
    }
}

/// Resolve the runtime status of a ref that has no `job_id` (so jobworkerp cannot be queried),
/// from the ref's own `enqueue_error` / `result_status`. Pure so the case breakdown is unit-tested
/// without a live jobworkerp server. Cases:
/// - `enqueue_error` set → `EnqueueFailed`: enqueue failed before a job_id was assigned.
/// - `result_status` set → that terminal status: a job finished and `update_result` recorded it
///   even though the job_id is absent (e.g. a Direct-fallback job).
/// - nothing recorded → `Running`: the job was enqueued but its job_id is not yet assigned (a
///   Direct-fallback runner only returns the job_id once its blocking execution resolves).
///   jobworkerp is already running it and the conductor is awaiting the result, so this is RUNNING
///   from the conductor's vantage point — not PENDING ("not started") nor UNKNOWN ("untrackable").
fn resolve_jobidless_status(
    enqueue_error: Option<&str>,
    result_status: Option<i32>,
) -> (i32, String) {
    if enqueue_error.is_some() {
        return (
            ResolvedExecutionStatus::EnqueueFailed as i32,
            "enqueue failed before job_id was assigned".to_string(),
        );
    }
    match result_status {
        Some(rs) => resolve_status_without_stored_result(Some(rs)),
        None => (
            ResolvedExecutionStatus::Running as i32,
            "enqueued; job_id not yet assigned; awaiting result on conductor side".to_string(),
        ),
    }
}

impl UseExecutionRefRepository for ExecutionStatusAppImpl {
    fn execution_ref_repository(&self) -> &ExecutionRefRepositoryImpl {
        &self.execution_ref_repository
    }
}

impl UseJobworkerpServerRepository for ExecutionStatusAppImpl {
    fn jobworkerp_server_repository(&self) -> &JobworkerpServerRepositoryImpl {
        &self.jobworkerp_server_repository
    }
}

#[async_trait]
impl ExecutionRefRecorder for ExecutionStatusAppImpl {
    async fn record_execution_ref(&self, execution_ref: ExecutionRef) -> Result<ExecutionRefId> {
        self.create_execution_ref(&execution_ref).await
    }
    async fn update_job_id(&self, id: &ExecutionRefId, job_id: i64) -> Result<()> {
        self.update_execution_ref_job_id(id, job_id).await
    }
    async fn update_result(
        &self,
        id: &ExecutionRefId,
        job_id: Option<i64>,
        result_status: i32,
    ) -> Result<()> {
        self.update_execution_ref_result(id, job_id, result_status)
            .await
    }
    async fn update_enqueue_error(&self, id: &ExecutionRefId, error: &str) -> Result<()> {
        self.update_execution_ref_enqueue_error(id, error).await
    }
}

#[async_trait]
impl ExecutionStatusApp for ExecutionStatusAppImpl {
    async fn create_execution_ref(&self, execution_ref: &ExecutionRef) -> Result<ExecutionRefId> {
        let db = self.execution_ref_repository().db_pool();
        let mut tx = db.begin().await?;
        let id = self
            .execution_ref_repository()
            .create(&mut *tx, execution_ref)
            .await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn update_execution_ref_job_id(&self, id: &ExecutionRefId, job_id: i64) -> Result<()> {
        // Single-statement best-effort update: run on the pool directly (autocommit) instead of
        // wrapping it in an explicit BEGIN/COMMIT round-trip.
        let db = self.execution_ref_repository().db_pool();
        self.execution_ref_repository()
            .update_job_id(db, id, job_id)
            .await
    }

    async fn update_execution_ref_result(
        &self,
        id: &ExecutionRefId,
        job_id: Option<i64>,
        result_status: i32,
    ) -> Result<()> {
        let db = self.execution_ref_repository().db_pool();
        self.execution_ref_repository()
            .update_result(db, id, job_id, result_status)
            .await
    }

    async fn update_execution_ref_enqueue_error(
        &self,
        id: &ExecutionRefId,
        error: &str,
    ) -> Result<()> {
        let db = self.execution_ref_repository().db_pool();
        self.execution_ref_repository()
            .update_enqueue_error(db, id, error)
            .await
    }

    async fn find_execution_ref(&self, id: &ExecutionRefId) -> Result<Option<ExecutionRef>> {
        self.execution_ref_repository().find(id).await
    }

    async fn find_latest_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
    ) -> Result<Option<ExecutionRef>> {
        self.execution_ref_repository()
            .find_latest_by_source(source_type, source_id)
            .await
    }

    async fn find_list_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<ExecutionRef>> {
        self.execution_ref_repository()
            .find_list_by_source(source_type, source_id, limit, offset)
            .await
    }

    async fn find_runtime_status(
        &self,
        id: &ExecutionRefId,
    ) -> Result<Option<ExecutionRuntimeStatus>> {
        Ok(match self.find_execution_ref(id).await? {
            Some(execution_ref) => Some(self.resolve_status(execution_ref).await),
            None => None,
        })
    }

    async fn find_latest_runtime_status_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
    ) -> Result<Option<ExecutionRuntimeStatus>> {
        Ok(
            match self.find_latest_by_source(source_type, source_id).await? {
                Some(execution_ref) => Some(self.resolve_status(execution_ref).await),
                None => None,
            },
        )
    }

    async fn cancel_execution(&self, id: &ExecutionRefId) -> Result<bool> {
        let execution_ref = self
            .find_execution_ref(id)
            .await?
            .context("execution_ref not found")?;
        let job_id = execution_ref
            .job_id
            .context("execution_ref.job_id is missing")?;
        let client = self.client_for(&execution_ref).await?;
        let metadata = HashMap::new();
        let is_success = client
            .jobworkerp_client
            .job_client()
            .await
            .delete(to_request(&metadata, JobId { value: job_id })?)
            .await
            .map(|r| r.into_inner().is_success)?;

        // jobworkerp's delete cancels (and discards) a PENDING job without producing a JobResult,
        // so without this the status API would later find neither a processing status nor a result
        // and infer Succeeded. Record the terminal Cancelled status so resolve_status reports it
        // correctly. Best-effort like the other recorders: a write failure must not turn a
        // successful cancellation into an error.
        if is_success {
            if let Err(e) = self
                .update_execution_ref_result(id, Some(job_id), ResultStatus::Cancelled as i32)
                .await
            {
                tracing::warn!(
                    "failed to record cancelled result_status for execution_ref id={}: {e:?}",
                    id.value
                );
            }
        }

        Ok(is_success)
    }

    async fn delete_execution_ref(&self, id: &ExecutionRefId) -> Result<DeleteResult> {
        // Resolve runtime status first; deleting an active or status-indeterminate ref would drop a
        // still-trackable execution from the ledger (plan §5.3).
        let Some(status) = self.find_runtime_status(id).await? else {
            return Ok(DeleteResult::NotFound);
        };
        let resolved = ResolvedExecutionStatus::try_from(status.resolved_status)
            .unwrap_or(ResolvedExecutionStatus::Unspecified);
        if !is_terminal(resolved) {
            return Ok(DeleteResult::NotTerminal);
        }
        if self.execution_ref_repository().delete(id).await? {
            Ok(DeleteResult::Deleted)
        } else {
            // Disappeared between status resolution and delete.
            Ok(DeleteResult::NotFound)
        }
    }

    async fn delete_execution_refs_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
        include_active: bool,
    ) -> Result<u64> {
        if include_active {
            // Force cleanup: skip status resolution entirely (works even when jobworkerp is down).
            return self
                .execution_ref_repository()
                .delete_all_by_source(source_type, source_id)
                .await;
        }
        // Terminal-only: resolve each ref and delete just the ones confirmed terminal. This issues
        // one jobworkerp round-trip per ref (and rebuilds the client each time via resolve_status →
        // client_for, even though all refs of a source share one server). Acceptable: delete-by-
        // source is an admin cleanup operation, not a hot path, and status must be resolved per ref.
        let refs = self
            .find_list_by_source(source_type, source_id, None, None)
            .await?;
        let mut terminal_ids = Vec::new();
        for execution_ref in refs {
            let id = execution_ref.id;
            let status = self.resolve_status(execution_ref).await;
            let resolved = ResolvedExecutionStatus::try_from(status.resolved_status)
                .unwrap_or(ResolvedExecutionStatus::Unspecified);
            if is_terminal(resolved) {
                if let Some(id) = id {
                    terminal_ids.push(id.value);
                }
            }
        }
        self.execution_ref_repository()
            .delete_by_ids(&terminal_ids)
            .await
    }

    async fn find_list(
        &self,
        filter: ExecutionRefListFilter,
        limit: Option<i32>,
        offset: Option<i64>,
    ) -> Result<Vec<ExecutionRef>> {
        self.execution_ref_repository()
            .find_list(&filter, limit, offset)
            .await
    }

    async fn count_list(&self, filter: ExecutionRefListFilter) -> Result<i64> {
        self.execution_ref_repository().count_list(&filter).await
    }

    async fn trigger_execution(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
        args_json: Option<String>,
    ) -> Result<TriggerOutcome> {
        // Resolve the source config (Unimplemented for non-Cron, NotFound for a missing config).
        let resolved = self.source_resolver.resolve(source_type, source_id).await?;
        // Override args replace (not merge) the configured args.
        let args = args_json.or(resolved.configured_args);

        // Create the pending ExecutionRef FIRST: the manual trigger's contract is to return a
        // cancellable id, so if recording fails we must not enqueue. Surface it as Internal.
        let now = Self::now();
        let pending = ExecutionRef {
            source_type: source_type as i32,
            source_id,
            source_name: resolved.source_name,
            jobworkerp_server_id: Some(resolved.jobworkerp_server_id),
            triggered_at: now,
            created_at: now,
            ..Default::default()
        };
        let id = self.create_execution_ref(&pending).await.map_err(|e| {
            anyhow::anyhow!("failed to create pending execution_ref before enqueue: {e}")
        })?;

        let plan = ExecutionPlan {
            endpoint: resolved.endpoint,
            target: resolved.target,
            args,
        };

        // Enqueue and detach terminal monitoring; returns the immediate job_id (Some) or None for a
        // Direct-fallback runner whose job_id is only known once the spawned task resolves it.
        match shared::spawn_and_record(self.recorder.clone(), id, plan).await {
            Ok(Some(_job_id)) => {
                // Streaming runner: job_id was recorded; resolve the live status (PENDING/RUNNING).
                let status = self
                    .find_runtime_status(&id)
                    .await?
                    .unwrap_or_else(|| self.running_status_without_job_id(&id, None));
                Ok(TriggerOutcome {
                    execution_ref_id: Some(id),
                    status,
                })
            }
            Ok(None) => {
                // Direct fallback: job_id not yet assigned (it arrives only when the blocking
                // execution resolves). The job is running and the conductor is awaiting its result,
                // so report RUNNING; it is not cancellable until the job_id is recorded.
                Ok(TriggerOutcome {
                    execution_ref_id: Some(id),
                    status: self.running_status_without_job_id(
                        &id,
                        Some("job_id pending; not cancellable until assigned".to_string()),
                    ),
                })
            }
            Err(_e) => {
                // Enqueue failed before a job_id was assigned; spawn_and_record already recorded the
                // enqueue_error. Report it as ENQUEUE_FAILED rather than failing the RPC.
                Ok(TriggerOutcome {
                    execution_ref_id: Some(id),
                    status: ExecutionRuntimeStatus {
                        execution_ref: self.find_execution_ref(&id).await.ok().flatten(),
                        resolved_status: ResolvedExecutionStatus::EnqueueFailed as i32,
                        status_source: ExecutionStatusSource::ExecutionRef as i32,
                        observed_at: Self::now(),
                        detail: Some("enqueue failed before job_id was assigned".to_string()),
                    },
                })
            }
        }
    }

    async fn re_execute(&self, id: &ExecutionRefId) -> Result<TriggerOutcome> {
        // The original ref must exist.
        let original = self.find_execution_ref(id).await?.ok_or_else(|| {
            UiEventHandlerError::NotFound(format!("execution_ref not found: id={}", id.value))
        })?;
        let source_type = ExecutionSourceType::try_from(original.source_type)
            .unwrap_or(ExecutionSourceType::Unspecified);
        // Cron only (same constraint as trigger_execution); other types are unimplemented.
        if source_type != ExecutionSourceType::CronScheduler {
            return Err(UiEventHandlerError::Unimplemented(format!(
                "re-execute is only supported for CRON_SCHEDULER, got {source_type:?}"
            ))
            .into());
        }
        // Re-run with the source's CURRENT config (args_json = None). If the source config has since
        // been deleted, the resolver reports NotFound; for re-execute that means "the original ref's
        // source no longer exists", which is a FailedPrecondition rather than a generic NotFound.
        match self
            .trigger_execution(source_type, original.source_id, None)
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(e) => match e.downcast_ref::<UiEventHandlerError>() {
                Some(UiEventHandlerError::NotFound(msg)) => {
                    Err(UiEventHandlerError::FailedPrecondition(format!(
                        "source config for the original execution_ref is gone: {msg}"
                    ))
                    .into())
                }
                _ => Err(e),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        infer_status_without_result, is_terminal, map_processing_status, map_result_status,
        resolve_jobidless_status, resolve_status_without_stored_result,
    };
    use jobworkerp_client::jobworkerp::data::{JobProcessingStatus, ResultStatus};
    use proto::jobworkerp_conductor::data::ResolvedExecutionStatus;

    // A job_id-less ref that was enqueued but has not yet recorded job_id/result (a Direct-fallback
    // job whose blocking execution has not resolved) is RUNNING: the conductor is awaiting its
    // result. Reporting PENDING/UNKNOWN here would make an in-flight manual trigger look un-started
    // or untrackable on a follow-up FindRuntimeStatus.
    #[test]
    fn jobidless_no_record_is_running() {
        let (status, _) = resolve_jobidless_status(None, None);
        assert_eq!(status, ResolvedExecutionStatus::Running as i32);
    }

    // enqueue_error wins: enqueue failed before a job_id existed.
    #[test]
    fn jobidless_with_enqueue_error_is_enqueue_failed() {
        let (status, _) = resolve_jobidless_status(Some("connection refused"), None);
        assert_eq!(status, ResolvedExecutionStatus::EnqueueFailed as i32);
        // enqueue_error takes precedence even if a result_status is somehow also present.
        let (status, _) =
            resolve_jobidless_status(Some("boom"), Some(ResultStatus::Success as i32));
        assert_eq!(status, ResolvedExecutionStatus::EnqueueFailed as i32);
    }

    // A recorded terminal result_status is honored even without a job_id (e.g. a finished
    // Direct-fallback job).
    #[test]
    fn jobidless_with_result_status_uses_that_terminal_status() {
        let (status, _) = resolve_jobidless_status(None, Some(ResultStatus::Cancelled as i32));
        assert_eq!(status, ResolvedExecutionStatus::Cancelled as i32);
        let (status, _) = resolve_jobidless_status(None, Some(ResultStatus::FatalError as i32));
        assert_eq!(status, ResolvedExecutionStatus::Failed as i32);
    }

    // Only confirmed-terminal statuses are deletable; active and indeterminate ones are protected
    // so a still-trackable / cancellable execution is never dropped from the ledger (plan §5.3).
    #[test]
    fn is_terminal_allows_only_terminal_statuses() {
        for s in [
            ResolvedExecutionStatus::Succeeded,
            ResolvedExecutionStatus::Failed,
            ResolvedExecutionStatus::Cancelled,
            ResolvedExecutionStatus::EnqueueFailed,
        ] {
            assert!(is_terminal(s), "{s:?} should be terminal");
        }
        for s in [
            ResolvedExecutionStatus::Unspecified,
            ResolvedExecutionStatus::Pending,
            ResolvedExecutionStatus::Running,
            ResolvedExecutionStatus::WaitResult,
            ResolvedExecutionStatus::Cancelling,
            ResolvedExecutionStatus::Unknown,
            ResolvedExecutionStatus::Unavailable,
        ] {
            assert!(!is_terminal(s), "{s:?} should be protected");
        }
    }

    #[test]
    fn maps_processing_status_to_resolved_status() {
        assert_eq!(
            map_processing_status(JobProcessingStatus::Pending as i32),
            ResolvedExecutionStatus::Pending as i32
        );
        assert_eq!(
            map_processing_status(JobProcessingStatus::Running as i32),
            ResolvedExecutionStatus::Running as i32
        );
        assert_eq!(
            map_processing_status(JobProcessingStatus::Cancelling as i32),
            ResolvedExecutionStatus::Cancelling as i32
        );
    }

    #[test]
    fn maps_result_status_to_terminal_status() {
        assert_eq!(
            map_result_status(ResultStatus::Success),
            ResolvedExecutionStatus::Succeeded as i32
        );
        assert_eq!(
            map_result_status(ResultStatus::Cancelled),
            ResolvedExecutionStatus::Cancelled as i32
        );
        assert_eq!(
            map_result_status(ResultStatus::FatalError),
            ResolvedExecutionStatus::Failed as i32
        );
    }

    // Legacy refs (no recorded terminal status) leave no result only when they succeeded
    // (workflow executions persist failures via store_failure), so they resolve to Succeeded.
    #[test]
    fn infers_succeeded_when_no_processing_status_or_result() {
        assert_eq!(
            infer_status_without_result(),
            ResolvedExecutionStatus::Succeeded as i32
        );
    }

    // A failed job that left no stored result (e.g. worker store_failure=false, or a cancelled
    // PENDING job) must NOT be reported as Succeeded: the recorded terminal status wins.
    #[test]
    fn no_stored_result_uses_recorded_failed_status() {
        let (status, _) =
            resolve_status_without_stored_result(Some(ResultStatus::FatalError as i32));
        assert_eq!(status, ResolvedExecutionStatus::Failed as i32);
    }

    #[test]
    fn no_stored_result_uses_recorded_cancelled_status() {
        let (status, _) =
            resolve_status_without_stored_result(Some(ResultStatus::Cancelled as i32));
        assert_eq!(status, ResolvedExecutionStatus::Cancelled as i32);
    }

    #[test]
    fn no_stored_result_uses_recorded_success_status() {
        let (status, _) = resolve_status_without_stored_result(Some(ResultStatus::Success as i32));
        assert_eq!(status, ResolvedExecutionStatus::Succeeded as i32);
    }

    // Without a recorded terminal status (legacy ref), fall back to the Succeeded inference.
    #[test]
    fn no_stored_result_falls_back_to_inference_for_legacy_ref() {
        let (status, _) = resolve_status_without_stored_result(None);
        assert_eq!(status, ResolvedExecutionStatus::Succeeded as i32);
    }

    // cancel_execution records `ResultStatus::Cancelled` on a successful PENDING-job delete (which
    // produces no JobResult). Guard that this exact recorded value flows through the no-result
    // resolution path to Cancelled, so the two stay in sync: if either side drifts, a cancelled job
    // would silently resolve to Succeeded again.
    #[test]
    fn recorded_cancel_value_resolves_to_cancelled() {
        let recorded = ResultStatus::Cancelled as i32;
        let (status, _) = resolve_status_without_stored_result(Some(recorded));
        assert_eq!(status, ResolvedExecutionStatus::Cancelled as i32);
    }
}
