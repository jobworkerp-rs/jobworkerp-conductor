//! End-to-end integration tests for the execution-status management RPCs (B2/B3/B5).
//!
//! These exercise `ExecutionStatusAppImpl` against a real RDB (a temporary SQLite file per
//! `setup()` call, so concurrent tests stay isolated). The error paths (Unimplemented / NotFound /
//! FailedPrecondition) and all of B3 (delete) are resolved before any jobworkerp call and run by
//! default — they form the regression net for this surface.
//!
//! Only the B2 happy-path test is `#[ignore]`d: it requires a reachable jobworkerp gRPC server and
//! a workflow URL it can fetch, supplied via
//! `TEST_JOBWORKERP_HOST` / `TEST_JOBWORKERP_PORT` / `TEST_WORKFLOW_URL` (or `TEST_WORKER_NAME`).
//! Run it with: `cargo test -p app --test execution_status_e2e -- --ignored`.

use std::sync::Arc;

use app::app::cron_scheduler::{CronSchedulerApp, CronSchedulerAppImpl};
use app::app::execution_status::{DeleteResult, ExecutionStatusApp, ExecutionStatusAppImpl};
use app::app::jobworkerp_server::{JobworkerpServerApp, JobworkerpServerAppImpl};
use app::app::notification::ConfigChangeNotificationServiceImpl;
use app::app::source_resolver::CronSourceResolver;
use infra::infra::cron_scheduler::rdb::CronSchedulerRepositoryImpl;
use infra::infra::execution_ref::rdb::{ExecutionRefRepository, ExecutionRefRepositoryImpl};
use infra::infra::jobworkerp_server::rdb::JobworkerpServerRepositoryImpl;
use infra::infra::IdGeneratorWrapper;
use infra_utils::infra::rdb::RdbPool;
use jobworkerp_client::jobworkerp::data::ResultStatus;
use memory_utils::cache::stretto::{new_memory_cache, MemoryCacheConfig};
use proto::jobworkerp_conductor::data::{
    cron_scheduler_data::ExecutionTarget, CronSchedulerData, ExecutionRef, ExecutionRefId,
    ExecutionSourceType, JobworkerpServerData, JobworkerpServerId, ResolvedExecutionStatus,
    WorkerExecution, WorkflowExecution,
};

#[cfg(feature = "mysql")]
const MYSQL_SCHEMA: &str = include_str!("../../infra/sql/mysql/002_schema.sql");
#[cfg(not(feature = "mysql"))]
const RDB_MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../infra/sql/sqlite");

/// Test harness: the wired app plus the repository handles used to seed/inspect rows directly.
struct Harness {
    app: ExecutionStatusAppImpl,
    server_app: JobworkerpServerAppImpl,
    cron_app: Arc<CronSchedulerAppImpl>,
    exec_repo: ExecutionRefRepositoryImpl,
    pool: &'static RdbPool,
}

/// Build an `ExecutionStatusAppImpl` over the shared test RDB, mirroring `AppModule::new_by_env`'s
/// wiring (one-way ExecutionStatus → Cron via `CronSourceResolver`).
async fn setup() -> Harness {
    let pool = setup_pool().await;
    clean_test_tables(pool).await;

    let mc_config = MemoryCacheConfig::default();
    let notification = Arc::new(
        ConfigChangeNotificationServiceImpl::new_memory_default().expect("in-memory notification"),
    );

    let exec_repo = ExecutionRefRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
    let server_repo = JobworkerpServerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
    let cron_repo = CronSchedulerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);

    let cron_app = Arc::new(CronSchedulerAppImpl::new(
        cron_repo,
        new_memory_cache(&mc_config),
        notification.clone(),
    ));
    let server_app = JobworkerpServerAppImpl::new(
        server_repo.clone(),
        new_memory_cache(&mc_config),
        notification.clone(),
    );
    let resolver = Arc::new(CronSourceResolver::new(
        cron_app.clone(),
        server_repo.clone(),
    ));
    let app = ExecutionStatusAppImpl::new(exec_repo.clone(), server_repo, resolver);

    Harness {
        app,
        server_app,
        cron_app,
        exec_repo,
        pool,
    }
}

#[cfg(not(feature = "mysql"))]
async fn setup_pool() -> &'static RdbPool {
    infra_utils::infra::test::setup_test_rdb_from(RDB_MIGRATIONS_DIR).await
}

#[cfg(feature = "mysql")]
async fn setup_pool() -> &'static RdbPool {
    let pool: &'static RdbPool = Box::leak(Box::new(
        infra_utils::infra::rdb::new_rdb_pool(&infra_utils::infra::test::MYSQL_CONFIG, None)
            .await
            .expect("init mysql pool"),
    ));
    sqlx::raw_sql(sqlx::AssertSqlSafe(MYSQL_SCHEMA.to_string()))
        .execute(pool)
        .await
        .expect("init mysql schema");
    pool
}

#[cfg(not(feature = "mysql"))]
async fn clean_test_tables(pool: &RdbPool) {
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "DELETE FROM `execution_ref`;
         DELETE FROM `cron_scheduler`;
         DELETE FROM `jobworkerp_server`;"
            .to_string(),
    ))
    .execute(pool)
    .await
    .expect("clean sqlite test tables");
}

#[cfg(feature = "mysql")]
async fn clean_test_tables(pool: &RdbPool) {
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "SET FOREIGN_KEY_CHECKS = 0;
         TRUNCATE TABLE `execution_ref`;
         TRUNCATE TABLE `cron_scheduler`;
         TRUNCATE TABLE `jobworkerp_server`;
         SET FOREIGN_KEY_CHECKS = 1;"
            .to_string(),
    ))
    .execute(pool)
    .await
    .expect("clean mysql test tables");
}

/// `TriggerOutcome` doesn't implement `Debug`, so `Result::expect_err` is unavailable. Collapse a
/// trigger/re-execute result into its error string (panicking if it unexpectedly succeeded).
fn expect_trigger_err(
    result: Result<app::app::execution_status::TriggerOutcome, anyhow::Error>,
    ctx: &str,
) -> String {
    match result {
        Ok(_) => panic!("{ctx}: expected an error but the call succeeded"),
        Err(e) => e.to_string(),
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

impl Harness {
    /// Seed a jobworkerp server row pointing at the given host/port; returns its id.
    async fn seed_server(&self, host: &str, port: &str) -> JobworkerpServerId {
        self.server_app
            .create_jobworkerp_server(&JobworkerpServerData {
                name: format!("test-server-{}", now()),
                host: host.to_string(),
                port: port.to_string(),
                ssl_enabled: false,
                description: None,
                enabled: true,
                created_at: now(),
                updated_at: now(),
            })
            .await
            .expect("seed server")
    }

    /// Seed a Cron scheduler row with the given execution target; returns its source_id.
    async fn seed_cron(&self, server_id: &JobworkerpServerId, target: ExecutionTarget) -> i64 {
        let id = self
            .cron_app
            .create_cron_scheduler(&CronSchedulerData {
                name: format!("test-cron-{}", now()),
                jobworkerp_server_id: Some(*server_id),
                workflow_url: String::new(),
                channel: None,
                crontab: "0 0 * * * *".to_string(),
                enabled: true,
                description: None,
                created_at: now(),
                updated_at: now(),
                args: None,
                execution_target: Some(target),
            })
            .await
            .expect("seed cron");
        id.value
    }

    /// Insert an execution_ref directly with a recorded terminal result_status (no jobworkerp call
    /// needed to resolve it as terminal).
    async fn seed_terminal_ref(
        &self,
        source_id: i64,
        server_id: &JobworkerpServerId,
        result_status: ResultStatus,
    ) -> ExecutionRefId {
        let mut tx = self.pool.begin().await.unwrap();
        let id = self
            .exec_repo
            .create(
                &mut *tx,
                &ExecutionRef {
                    source_type: ExecutionSourceType::CronScheduler as i32,
                    source_id,
                    source_name: "test".to_string(),
                    jobworkerp_server_id: Some(*server_id),
                    triggered_at: now(),
                    created_at: now(),
                    result_status: Some(result_status as i32),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    }
}

// ---------------------------------------------------------------------------
// B2: TriggerExecution — error paths (no jobworkerp required)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trigger_non_cron_source_is_unimplemented() {
    let h = setup().await;
    let err = expect_trigger_err(
        h.app
            .trigger_execution(ExecutionSourceType::SlackEventHandler, 1, None)
            .await,
        "slack trigger",
    );
    assert!(
        err.contains("Unimplemented"),
        "expected Unimplemented, got: {err}"
    );

    let err = expect_trigger_err(
        h.app
            .trigger_execution(ExecutionSourceType::WorkerResultHandler, 1, None)
            .await,
        "worker-result trigger",
    );
    assert!(err.contains("Unimplemented"), "got: {err}");
}

#[tokio::test]
async fn trigger_missing_cron_config_is_not_found() {
    let h = setup().await;
    let err = expect_trigger_err(
        h.app
            .trigger_execution(ExecutionSourceType::CronScheduler, 999_999, None)
            .await,
        "missing cron trigger",
    );
    assert!(err.contains("NotFound"), "expected NotFound, got: {err}");
}

// ---------------------------------------------------------------------------
// B3: Delete — full e2e (no jobworkerp required; terminal resolved from result_status)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_terminal_ref_succeeds() {
    let h = setup().await;
    let server = h.seed_server("localhost", "9000").await;
    // The fixture has no job_id, so the terminal result is resolved without contacting jobworkerp.
    let id = h.seed_terminal_ref(1, &server, ResultStatus::Success).await;

    let result = h.app.delete_execution_ref(&id).await.unwrap();
    assert_eq!(result, DeleteResult::Deleted);
    // Gone afterwards.
    assert!(h.app.find_execution_ref(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn delete_missing_ref_is_not_found() {
    let h = setup().await;
    let result = h
        .app
        .delete_execution_ref(&ExecutionRefId { value: 424242 })
        .await
        .unwrap();
    assert_eq!(result, DeleteResult::NotFound);
}

#[tokio::test]
async fn delete_by_source_counts_only_terminal_when_not_forced() {
    let h = setup().await;
    let server = h.seed_server("localhost", "9000").await;
    let source_id = 7;
    // Two terminal refs for the same source.
    h.seed_terminal_ref(source_id, &server, ResultStatus::Success)
        .await;
    h.seed_terminal_ref(source_id, &server, ResultStatus::ErrorAndRetry)
        .await;

    let deleted = h
        .app
        .delete_execution_refs_by_source(ExecutionSourceType::CronScheduler, source_id, false)
        .await
        .unwrap();
    assert_eq!(deleted, 2, "both terminal refs should be deleted");

    // Nothing left for the source.
    let remaining = h
        .app
        .find_list_by_source(ExecutionSourceType::CronScheduler, source_id, None, None)
        .await
        .unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn delete_by_source_force_bypasses_status() {
    let h = setup().await;
    let server = h.seed_server("localhost", "9000").await;
    let source_id = 8;
    // A ref with no result_status and no job_id: resolve_status cannot confirm terminal, so
    // include_active=false would protect it; include_active=true must delete it regardless.
    {
        let mut tx = h.pool.begin().await.unwrap();
        h.exec_repo
            .create(
                &mut *tx,
                &ExecutionRef {
                    source_type: ExecutionSourceType::CronScheduler as i32,
                    source_id,
                    source_name: "active".to_string(),
                    jobworkerp_server_id: Some(server),
                    triggered_at: now(),
                    created_at: now(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let deleted = h
        .app
        .delete_execution_refs_by_source(ExecutionSourceType::CronScheduler, source_id, true)
        .await
        .unwrap();
    assert_eq!(deleted, 1, "force delete must bypass status protection");
}

// ---------------------------------------------------------------------------
// B5: ReExecute — error paths (no jobworkerp required)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn re_execute_missing_ref_is_not_found() {
    let h = setup().await;
    let err = expect_trigger_err(
        h.app.re_execute(&ExecutionRefId { value: 777 }).await,
        "re-execute missing ref",
    );
    assert!(err.contains("NotFound"), "got: {err}");
}

#[tokio::test]
async fn re_execute_non_cron_ref_is_unimplemented() {
    let h = setup().await;
    let server = h.seed_server("localhost", "9000").await;
    // A Slack-sourced ref: re-execute must reject it as Unimplemented.
    let id = {
        let mut tx = h.pool.begin().await.unwrap();
        let id = h
            .exec_repo
            .create(
                &mut *tx,
                &ExecutionRef {
                    source_type: ExecutionSourceType::SlackEventHandler as i32,
                    source_id: 1,
                    source_name: "slack".to_string(),
                    jobworkerp_server_id: Some(server),
                    triggered_at: now(),
                    created_at: now(),
                    result_status: Some(ResultStatus::Success as i32),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    };

    let err = expect_trigger_err(h.app.re_execute(&id).await, "re-execute non-Cron ref");
    assert!(err.contains("Unimplemented"), "got: {err}");
}

#[tokio::test]
async fn re_execute_with_deleted_source_is_failed_precondition() {
    let h = setup().await;
    let server = h.seed_server("localhost", "9000").await;
    // A Cron-sourced ref whose source_id has no matching cron config (deleted): re-execute resolves
    // the config, gets NotFound, and remaps it to FailedPrecondition ("source no longer exists").
    let id = h
        .seed_terminal_ref(123_456, &server, ResultStatus::Success)
        .await;

    let err = expect_trigger_err(h.app.re_execute(&id).await, "re-execute deleted source");
    assert!(
        err.contains("FailedPrecondition"),
        "expected FailedPrecondition, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// B2: TriggerExecution — happy path (requires a reachable jobworkerp + fetchable workflow URL)
// ---------------------------------------------------------------------------

/// Resolve the jobworkerp endpoint and workflow target from env; `None` skips the happy-path test.
fn jobworkerp_target() -> Option<(String, String, ExecutionTarget)> {
    let host = std::env::var("TEST_JOBWORKERP_HOST").ok()?;
    let port = std::env::var("TEST_JOBWORKERP_PORT").unwrap_or_else(|_| "9000".to_string());
    // Prefer a worker name (executes any runner) if given; else a workflow URL.
    if let Ok(worker) = std::env::var("TEST_WORKER_NAME") {
        let using = std::env::var("TEST_WORKER_USING").ok();
        return Some((
            host,
            port,
            ExecutionTarget::Worker(WorkerExecution {
                worker_name: worker,
                using,
            }),
        ));
    }
    let workflow_url = std::env::var("TEST_WORKFLOW_URL").ok()?;
    Some((
        host,
        port,
        ExecutionTarget::Workflow(WorkflowExecution {
            workflow_url,
            channel: None,
        }),
    ))
}

#[tokio::test]
#[ignore = "requires a reachable jobworkerp + TEST_JOBWORKERP_HOST/TEST_WORKFLOW_URL (or TEST_WORKER_NAME)"]
async fn trigger_cron_happy_path_records_ref() {
    let Some((host, port, target)) = jobworkerp_target() else {
        eprintln!(
            "skipping: set TEST_JOBWORKERP_HOST and TEST_WORKFLOW_URL (or TEST_WORKER_NAME) to run"
        );
        return;
    };
    let h = setup().await;
    let server = h.seed_server(&host, &port).await;
    let source_id = h.seed_cron(&server, target).await;

    let outcome = h
        .app
        .trigger_execution(ExecutionSourceType::CronScheduler, source_id, None)
        .await
        .expect("trigger should create a ref and enqueue");

    let ref_id = outcome
        .execution_ref_id
        .expect("a pending ExecutionRef id must be returned");

    // The ref is persisted and queryable immediately (terminal monitoring is detached).
    let stored = h.app.find_execution_ref(&ref_id).await.unwrap();
    assert!(stored.is_some(), "ExecutionRef must be persisted");

    // Right after enqueue the status is non-terminal (PENDING/RUNNING/WAIT_RESULT). For a streaming
    // runner the job_id is recorded; for Direct fallback it arrives via the detached task.
    let resolved = outcome.status.resolved_status;
    assert!(
        resolved == ResolvedExecutionStatus::Pending as i32
            || resolved == ResolvedExecutionStatus::Running as i32
            || resolved == ResolvedExecutionStatus::WaitResult as i32,
        "unexpected immediate status: {resolved}"
    );

    // Clean up the row we created (leave the detached terminal task to finish on its own).
    let _ = h
        .app
        .delete_execution_refs_by_source(ExecutionSourceType::CronScheduler, source_id, true)
        .await;
}
