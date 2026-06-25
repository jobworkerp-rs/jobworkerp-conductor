use anyhow::Result;
use chrono_tz::Tz;
use proto::jobworkerp_conductor::data::cron_scheduler_data::ExecutionTarget;
use proto::jobworkerp_conductor::data::{
    CronScheduler, ExecutionRef, ExecutionSourceType, JobworkerpServerId,
};
use shared::config_events_proto::ConfigChangeEventWrapper;
use shared::SharedLocalConfigStore;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

/// Resolve the timezone used to interpret cron expressions.
///
/// Why: tokio-cron-scheduler defaults to UTC, which surprises operators who
/// expect local-time semantics (e.g. `0 0 */6 * * *` firing every 6h on local
/// boundaries). Reads `CONDUCTOR_CRON_TIMEZONE` first, falls back to `TZ`,
/// then UTC. An invalid value is logged and falls back to UTC rather than
/// failing job registration.
fn resolve_cron_timezone() -> Tz {
    fn read_non_empty(key: &str) -> Option<String> {
        std::env::var(key)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    }

    let raw = read_non_empty("CONDUCTOR_CRON_TIMEZONE").or_else(|| read_non_empty("TZ"));

    match raw {
        Some(name) => match name.parse::<Tz>() {
            Ok(tz) => tz,
            Err(e) => {
                tracing::warn!(
                    "Invalid cron timezone '{}': {}. Falling back to UTC.",
                    name,
                    e
                );
                Tz::UTC
            }
        },
        None => Tz::UTC,
    }
}

/// 動的スケジューラー管理（プロトタイプ実装）
///
/// tokio-cron-schedulerの動的制御安全性を検証するための
/// プロトタイプ実装。完全停止→再起動方式を採用。
pub struct DynamicSchedulerManager {
    scheduler: JobScheduler,
    active_jobs: Arc<Mutex<HashMap<proto::jobworkerp_conductor::data::CronSchedulerId, JobHandle>>>,
    local_config_store: SharedLocalConfigStore,
    execution_ref_recorder: shared::SharedExecutionRefRecorder,
}

#[derive(Debug, Clone)]
struct JobHandle {
    name: String,
    job_id: Uuid,
    #[allow(dead_code)]
    config_hash: String,
}

impl DynamicSchedulerManager {
    pub async fn new() -> Result<Self> {
        let scheduler = JobScheduler::new().await?;

        Ok(Self {
            scheduler,
            active_jobs: Arc::new(Mutex::new(HashMap::new())),
            local_config_store: std::sync::Arc::new(std::sync::RwLock::new(
                shared::LocalConfigStore::new(),
            )),
            execution_ref_recorder: shared::noop_execution_ref_recorder(),
        })
    }

    pub async fn new_with_local_config(
        local_config_store: SharedLocalConfigStore,
        execution_ref_recorder: shared::SharedExecutionRefRecorder,
    ) -> Self {
        let scheduler = JobScheduler::new().await.unwrap();

        Self {
            scheduler,
            active_jobs: Arc::new(Mutex::new(HashMap::new())),
            local_config_store,
            execution_ref_recorder,
        }
    }

    /// スケジューラー開始
    pub async fn start(&self) -> Result<()> {
        self.scheduler.start().await?;
        tracing::info!("DynamicSchedulerManager started");
        Ok(())
    }

    /// 全ジョブ停止（安全な動的制御のための完全停止）
    pub async fn stop_all(&self) -> Result<()> {
        tracing::info!("Stopping all scheduler jobs for safe reconfiguration");

        // ロック取得してジョブリストを取得
        let jobs_to_remove = {
            let mut jobs = self.active_jobs.lock().unwrap();
            let jobs_list: Vec<_> = jobs.drain().collect();
            jobs_list
        };

        // ロック外でスケジューラーから削除
        for (id, handle) in jobs_to_remove {
            if let Err(e) = self.scheduler.remove(&handle.job_id).await {
                tracing::warn!(
                    "Failed to remove job id={}, name={}: {:?}",
                    id.value,
                    handle.name,
                    e
                );
            } else {
                tracing::debug!("Removed job: id={}, name={}", id.value, handle.name);
            }
        }

        tracing::info!("All scheduler jobs stopped successfully");
        Ok(())
    }

    /// アクティブジョブ数取得
    pub fn active_job_count(&self) -> usize {
        self.active_jobs.lock().unwrap().len()
    }

    /// 安全な設定リロード（完全停止→再起動方式）
    pub async fn safe_reload(&self, schedulers: Vec<CronScheduler>) -> Result<()> {
        tracing::info!("Starting safe scheduler reload");

        // Step 1: 全ジョブ停止
        self.stop_all().await?;

        // Step 2: 新しい設定でジョブ追加
        for scheduler in schedulers {
            if let Some(id) = &scheduler.id {
                self.add_scheduler_from_local(id).await?;
            }
        }

        tracing::info!("Safe scheduler reload completed");
        Ok(())
    }

    /// ローカル設定から新しいスケジューラーを追加（IDベース）
    pub async fn add_scheduler_from_local(
        &self,
        scheduler_id: &proto::jobworkerp_conductor::data::CronSchedulerId,
    ) -> Result<()> {
        let scheduler = {
            let store = self.local_config_store.read().unwrap();
            store
                .get_cron_scheduler(scheduler_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("CronScheduler id={} not found", scheduler_id.value)
                })?
                .clone()
        };

        let data = scheduler
            .data
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("CronScheduler data is missing"))?;

        // 🚀 安全性チェック: enabledがfalseの場合は追加しない
        if !data.enabled {
            tracing::debug!(
                "CronScheduler id={} is disabled, skipping",
                scheduler_id.value
            );
            return Ok(());
        }

        let name = data.name.clone();
        let jobworkerp_server_id = data
            .jobworkerp_server_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("CronScheduler jobworkerp_server_id is missing"))?;
        let cron_expr = data.crontab.clone();
        let args = data.args.clone();

        // Find the jobworkerp server endpoint from local config store
        let jobworkerp_endpoint = {
            let store = self
                .local_config_store
                .read()
                .map_err(|e| anyhow::anyhow!("Failed to read config store: {}", e))?;

            let server = store
                .get_jobworkerp_server(jobworkerp_server_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "JobworkerpServer with id {} not found",
                        jobworkerp_server_id.value
                    )
                })?;

            let server_data = server
                .data
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("JobworkerpServer data is missing"))?;

            let protocol = if server_data.ssl_enabled {
                "https"
            } else {
                "http"
            };
            format!("{}://{}:{}", protocol, server_data.host, server_data.port)
        };

        let execution_target = data.execution_target.clone();
        let workflow_url_fallback = data.workflow_url.clone();
        let channel_fallback = data.channel.clone();
        let recorder = self.execution_ref_recorder.clone();
        let scheduler_id_value = scheduler_id.value;
        let jobworkerp_server_id_value = jobworkerp_server_id.value;

        let job_name = name.to_string();
        let tz = resolve_cron_timezone();
        let job = Job::new_async_tz(cron_expr.as_str(), tz, move |_uuid, _l| {
            let execution_target = execution_target.clone();
            let workflow_url_fallback = workflow_url_fallback.clone();
            let channel_fallback = channel_fallback.clone();
            let jobworkerp_endpoint = jobworkerp_endpoint.clone();
            let job_name = job_name.clone();
            let args = args.clone();
            let recorder = recorder.clone();

            Box::pin(async move {
                // Capture the enqueue time before the (streaming) enqueue so triggered_at reflects
                // submission, not completion.
                let triggered_at = chrono::Utc::now().timestamp();

                // Normalize the cron execution_target (+ deprecated workflow_url fallback) into the
                // shared ResolvedTarget, then enqueue via the shared streaming-first dispatch. The
                // job_id is returned immediately (when the runner supports streaming) so the running
                // cron job is trackable / cancellable while it executes.
                let resolved_target = shared::ResolvedTarget::from_cron_target(
                    &execution_target,
                    &workflow_url_fallback,
                    channel_fallback.as_deref(),
                );
                let enqueue = async {
                    let target = resolved_target
                        .ok_or_else(|| anyhow::anyhow!("No execution target specified"))?;
                    shared::enqueue_by_target(&shared::ExecutionPlan {
                        endpoint: jobworkerp_endpoint.clone(),
                        target,
                        args: args.clone(),
                    })
                    .await
                };

                let pending = ExecutionRef {
                    source_type: ExecutionSourceType::CronScheduler as i32,
                    source_id: scheduler_id_value,
                    source_name: job_name.clone(),
                    jobworkerp_server_id: Some(JobworkerpServerId {
                        value: jobworkerp_server_id_value,
                    }),
                    triggered_at,
                    created_at: triggered_at,
                    ..Default::default()
                };

                match shared::record_pending_then_update(&recorder, pending, enqueue).await {
                    Ok(outcome) if outcome.success => {
                        tracing::info!("Successfully executed job: {}", job_name);
                    }
                    Ok(_) => {
                        tracing::warn!(
                            "Cron job {} failed terminally (job recorded for status tracking)",
                            job_name
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to enqueue job {}: {}", job_name, e);
                    }
                }
            })
        })?;

        let job_id = self.scheduler.add(job).await?;
        let config_hash = Self::compute_config_hash(&scheduler);

        let handle = JobHandle {
            name: name.to_string(),
            job_id,
            config_hash,
        };

        self.active_jobs
            .lock()
            .unwrap()
            .insert(*scheduler_id, handle);
        tracing::info!(
            "Added CronScheduler: id={}, name={}",
            scheduler_id.value,
            name
        );

        Ok(())
    }

    /// イベントからスケジューラーを更新（IDベース）
    pub async fn update_scheduler_from_event(
        &self,
        config_event: &ConfigChangeEventWrapper,
    ) -> Result<()> {
        use proto::jobworkerp_conductor::data::ChangeAction;
        use shared::config_events_proto::EntityId;

        let scheduler_id = match config_event.typed_id() {
            Some(EntityId::CronScheduler(id)) => id,
            _ => {
                return Err(anyhow::anyhow!(
                    "CronScheduler event has no id or wrong type"
                ))
            }
        };

        match config_event.action() {
            ChangeAction::Created | ChangeAction::Updated => {
                // 既存のジョブを削除してから再追加
                self.remove_scheduler_by_id(&scheduler_id).await?;
                self.add_scheduler_from_local(&scheduler_id).await?;
            }
            ChangeAction::Deleted => {
                self.remove_scheduler_by_id(&scheduler_id).await?;
            }
            _ => {}
        }

        Ok(())
    }

    /// IDベースでスケジューラーを削除
    async fn remove_scheduler_by_id(
        &self,
        scheduler_id: &proto::jobworkerp_conductor::data::CronSchedulerId,
    ) -> Result<()> {
        let handle = { self.active_jobs.lock().unwrap().remove(scheduler_id) };

        if let Some(handle) = handle {
            self.scheduler.remove(&handle.job_id).await?;
            tracing::info!(
                "Removed CronScheduler: id={}, name={}",
                scheduler_id.value,
                handle.name
            );
        }

        Ok(())
    }

    /// 設定ハッシュを計算
    fn compute_config_hash(scheduler: &CronScheduler) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        if let Some(data) = &scheduler.data {
            data.name.hash(&mut hasher);
            data.crontab.hash(&mut hasher);
            data.jobworkerp_server_id.hash(&mut hasher);
            data.enabled.hash(&mut hasher);
            data.args.hash(&mut hasher);
            match &data.execution_target {
                Some(ExecutionTarget::Workflow(wf)) => {
                    "workflow".hash(&mut hasher);
                    wf.workflow_url.hash(&mut hasher);
                    wf.channel.hash(&mut hasher);
                }
                Some(ExecutionTarget::Worker(w)) => {
                    "worker".hash(&mut hasher);
                    w.worker_name.hash(&mut hasher);
                    w.r#using.hash(&mut hasher);
                }
                None => {
                    // Backward compat: DB-loaded data always has execution_target
                    // (see CronSchedulerRow::to_proto). This branch only handles
                    // stale proto data that predates the oneof migration.
                    "legacy".hash(&mut hasher);
                    data.workflow_url.hash(&mut hasher);
                    data.channel.hash(&mut hasher);
                }
            }
        }

        format!("{:x}", hasher.finish())
    }

    /// テストジョブ追加（検証用）
    #[cfg(test)]
    pub async fn add_test_job(&self, id: i64, name: String, cron_expr: String) -> Result<()> {
        let job_name = name.clone();
        let tz = resolve_cron_timezone();
        let job = Job::new_async_tz(cron_expr.as_str(), tz, move |_uuid, _l| {
            let job_name = job_name.clone();
            Box::pin(async move {
                tracing::info!("Executing test job: {}", job_name);
            })
        })?;

        let job_id = self.scheduler.add(job).await?;

        let handle = JobHandle {
            name: name.clone(),
            job_id,
            config_hash: "test".to_string(),
        };

        let scheduler_id = proto::jobworkerp_conductor::data::CronSchedulerId { value: id };
        self.active_jobs
            .lock()
            .unwrap()
            .insert(scheduler_id, handle);
        tracing::info!(
            "Added test job: id={}, name={} with cron: {}",
            id,
            name,
            cron_expr
        );

        Ok(())
    }
    /// テスト専用の安全な設定リロード（テスト用）
    #[cfg(test)]
    pub async fn safe_reload_test(&self, test_jobs: Vec<(i64, String, String)>) -> Result<()> {
        tracing::info!("Starting safe scheduler reload (TEST MODE)");

        // Step 1: 全ジョブ停止
        self.stop_all().await?;

        // Step 2: テスト用ジョブ追加
        for (id, name, cron_expr) in test_jobs {
            self.add_test_job(id, name, cron_expr).await?;
        }

        tracing::info!("Safe scheduler reload completed (TEST MODE)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    // Serialize env-var tests: they mutate process-global state and would
    // race with each other or with concurrent reads in other tests.
    static ENV_LOCK: StdMutex<()> = StdMutex::new(());

    fn with_env<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(k, _)| (k.to_string(), std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
        f();
        for (k, v) in saved {
            match v {
                Some(val) => std::env::set_var(&k, val),
                None => std::env::remove_var(&k),
            }
        }
    }

    #[test]
    fn resolve_cron_timezone_defaults_to_utc() {
        with_env(&[("CONDUCTOR_CRON_TIMEZONE", None), ("TZ", None)], || {
            assert_eq!(resolve_cron_timezone(), Tz::UTC)
        });
    }

    #[test]
    fn resolve_cron_timezone_uses_conductor_var() {
        with_env(
            &[
                ("CONDUCTOR_CRON_TIMEZONE", Some("Asia/Tokyo")),
                ("TZ", Some("Europe/Paris")),
            ],
            || assert_eq!(resolve_cron_timezone(), chrono_tz::Asia::Tokyo),
        );
    }

    #[test]
    fn resolve_cron_timezone_falls_back_to_tz_var() {
        with_env(
            &[
                ("CONDUCTOR_CRON_TIMEZONE", None),
                ("TZ", Some("Europe/Paris")),
            ],
            || assert_eq!(resolve_cron_timezone(), chrono_tz::Europe::Paris),
        );
    }

    #[test]
    fn resolve_cron_timezone_invalid_value_falls_back_to_utc() {
        with_env(
            &[
                ("CONDUCTOR_CRON_TIMEZONE", Some("Not/A_Zone")),
                ("TZ", None),
            ],
            || assert_eq!(resolve_cron_timezone(), Tz::UTC),
        );
    }

    #[test]
    fn resolve_cron_timezone_empty_string_falls_back() {
        with_env(
            &[
                ("CONDUCTOR_CRON_TIMEZONE", Some("   ")),
                ("TZ", Some("Asia/Tokyo")),
            ],
            || assert_eq!(resolve_cron_timezone(), chrono_tz::Asia::Tokyo),
        );
    }

    #[tokio::test]
    async fn test_scheduler_creation() {
        let manager = DynamicSchedulerManager::new().await.unwrap();
        assert_eq!(manager.active_job_count(), 0);
    }

    #[tokio::test]
    async fn test_safe_dynamic_control() {
        let manager = DynamicSchedulerManager::new().await.unwrap();
        manager.start().await.unwrap();

        // 初期ジョブ追加
        manager
            .add_test_job(1, "test1".to_string(), "0/5 * * * * *".to_string())
            .await
            .unwrap();
        manager
            .add_test_job(2, "test2".to_string(), "0/10 * * * * *".to_string())
            .await
            .unwrap();
        assert_eq!(manager.active_job_count(), 2);

        // 安全なリロード
        let new_jobs = vec![
            (3, "test3".to_string(), "0/3 * * * * *".to_string()),
            (4, "test4".to_string(), "0/7 * * * * *".to_string()),
            (5, "test5".to_string(), "0/15 * * * * *".to_string()),
        ];

        manager.safe_reload_test(new_jobs).await.unwrap();
        assert_eq!(manager.active_job_count(), 3);

        // 全停止テスト
        manager.stop_all().await.unwrap();
        assert_eq!(manager.active_job_count(), 0);
    }
}
