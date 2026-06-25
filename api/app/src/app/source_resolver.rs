//! Source resolution + a DB-only ExecutionRef recorder for the manual trigger (B2).
//!
//! [`ExecutionSourceResolver`] turns a `(source_type, source_id)` into the concrete
//! [`shared::ResolvedTarget`] + endpoint + configured args needed to enqueue, keeping the
//! `ExecutionStatusApp` dependency on the config apps one-directional (ExecutionStatus → Cron, never
//! the reverse) so there is no dependency cycle.
//!
//! [`DbExecutionRefRecorder`] is the [`shared::ExecutionRefRecorder`] passed to
//! [`shared::spawn_and_record`]'s detached terminal task; it only touches the execution_ref
//! repository, so it can be cloned into a spawned task without dragging in the whole app.

use crate::app::cron_scheduler::{CronSchedulerApp, CronSchedulerAppImpl};
use anyhow::Result;
use async_trait::async_trait;
use infra::error::UiEventHandlerError;
use infra::infra::execution_ref::rdb::{ExecutionRefRepository, ExecutionRefRepositoryImpl};
use infra::infra::jobworkerp_server::rdb::{
    JobworkerpServerRepository, JobworkerpServerRepositoryImpl,
};
use infra_utils::infra::rdb::UseRdbPool;
use proto::jobworkerp_conductor::data::{
    ExecutionRef, ExecutionRefId, ExecutionSourceType, JobworkerpServerId,
};
use std::sync::Arc;

/// The execution information resolved from a source configuration: where/what to run plus the
/// identity fields needed to record the pending ExecutionRef.
pub struct ResolvedSource {
    pub endpoint: String,
    pub target: shared::ResolvedTarget,
    pub configured_args: Option<String>,
    pub source_name: String,
    pub jobworkerp_server_id: JobworkerpServerId,
}

#[async_trait]
pub trait ExecutionSourceResolver: Send + Sync {
    /// Resolve a source config into a [`ResolvedSource`]. Returns `UiEventHandlerError::Unimplemented`
    /// for source types not yet supported by the manual trigger, and `NotFound` when the config (or
    /// its server / target) is missing.
    async fn resolve(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
    ) -> Result<ResolvedSource>;
}

/// Resolves CRON_SCHEDULER sources. Holds a one-way `Arc` to the cron app (and the server repo);
/// nothing here points back at `ExecutionStatusApp`.
pub struct CronSourceResolver {
    cron_scheduler_app: Arc<CronSchedulerAppImpl>,
    jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
}

impl CronSourceResolver {
    pub fn new(
        cron_scheduler_app: Arc<CronSchedulerAppImpl>,
        jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
    ) -> Self {
        Self {
            cron_scheduler_app,
            jobworkerp_server_repository,
        }
    }

    /// Build the gRPC endpoint string for a jobworkerp server, mirroring
    /// `ExecutionStatusAppImpl::client_for`.
    async fn endpoint_for(&self, server_id: &JobworkerpServerId) -> Result<String> {
        let server = self
            .jobworkerp_server_repository
            .find(server_id)
            .await?
            .ok_or_else(|| {
                UiEventHandlerError::NotFound(format!(
                    "jobworkerp server not found: id={}",
                    server_id.value
                ))
            })?;
        let data = server.data.ok_or_else(|| {
            UiEventHandlerError::NotFound("jobworkerp server data is missing".to_string())
        })?;
        Ok(crate::app::execution_status::server_endpoint(&data))
    }
}

#[async_trait]
impl ExecutionSourceResolver for CronSourceResolver {
    async fn resolve(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
    ) -> Result<ResolvedSource> {
        // MVP: only Cron is supported. Slack / WorkerResult need a synthetic event / result to build
        // args and are explicitly unimplemented here (plan §4.3.1).
        if source_type != ExecutionSourceType::CronScheduler {
            return Err(UiEventHandlerError::Unimplemented(format!(
                "manual trigger is only supported for CRON_SCHEDULER, got {source_type:?}"
            ))
            .into());
        }

        let cron = self
            .cron_scheduler_app
            .find_cron_scheduler(
                &proto::jobworkerp_conductor::data::CronSchedulerId { value: source_id },
                None,
            )
            .await?
            .ok_or_else(|| {
                UiEventHandlerError::NotFound(format!("cron scheduler not found: id={source_id}"))
            })?;
        let data = cron.data.ok_or_else(|| {
            UiEventHandlerError::NotFound("cron scheduler data is missing".to_string())
        })?;
        let server_id = data.jobworkerp_server_id.ok_or_else(|| {
            UiEventHandlerError::NotFound(
                "cron scheduler jobworkerp_server_id is missing".to_string(),
            )
        })?;
        let endpoint = self.endpoint_for(&server_id).await?;
        let target = shared::ResolvedTarget::from_cron_target(
            &data.execution_target,
            &data.workflow_url,
            data.channel.as_deref(),
        )
        .ok_or_else(|| {
            UiEventHandlerError::FailedPrecondition(
                "cron scheduler has no execution target configured".to_string(),
            )
        })?;

        Ok(ResolvedSource {
            endpoint,
            target,
            configured_args: data.args,
            source_name: data.name,
            jobworkerp_server_id: server_id,
        })
    }
}

/// A minimal [`shared::ExecutionRefRecorder`] backed only by the execution_ref repository. Used by
/// the manual trigger's detached terminal task, where carrying the full app (and its other
/// dependencies) into a `tokio::spawn` would be needless.
#[derive(Clone)]
pub struct DbExecutionRefRecorder {
    execution_ref_repository: ExecutionRefRepositoryImpl,
}

impl DbExecutionRefRecorder {
    pub fn new(execution_ref_repository: ExecutionRefRepositoryImpl) -> Self {
        Self {
            execution_ref_repository,
        }
    }
}

#[async_trait]
impl shared::ExecutionRefRecorder for DbExecutionRefRecorder {
    async fn record_execution_ref(&self, execution_ref: ExecutionRef) -> Result<ExecutionRefId> {
        let db = self.execution_ref_repository.db_pool();
        let mut tx = db.begin().await?;
        let id = self
            .execution_ref_repository
            .create(&mut *tx, &execution_ref)
            .await?;
        tx.commit().await?;
        Ok(id)
    }
    async fn update_job_id(&self, id: &ExecutionRefId, job_id: i64) -> Result<()> {
        let db = self.execution_ref_repository.db_pool();
        self.execution_ref_repository
            .update_job_id(db, id, job_id)
            .await
    }
    async fn update_result(
        &self,
        id: &ExecutionRefId,
        job_id: Option<i64>,
        result_status: i32,
    ) -> Result<()> {
        let db = self.execution_ref_repository.db_pool();
        self.execution_ref_repository
            .update_result(db, id, job_id, result_status)
            .await
    }
    async fn update_enqueue_error(&self, id: &ExecutionRefId, error: &str) -> Result<()> {
        let db = self.execution_ref_repository.db_pool();
        self.execution_ref_repository
            .update_enqueue_error(db, id, error)
            .await
    }
}
