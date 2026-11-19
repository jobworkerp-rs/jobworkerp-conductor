//! TOML import, export and clear-all against a real RDB, with the in-memory
//! change notifications the running handlers listen to.
//!
//! The tests share one database, so each takes `LOCK` and starts from empty
//! tables. Run with `cargo test -p app --test config_management_e2e`
//! (add `--features mysql` and `TEST_MYSQL_URL` for MySQL).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use app::app::config_management::{ConfigManagementApp, ConfigManagementAppImpl, ImportReport};
use app::app::cron_scheduler::{CronSchedulerApp, CronSchedulerAppImpl};
use app::app::jobworkerp_server::{JobworkerpServerApp, JobworkerpServerAppImpl};
use app::app::notification::ConfigChangeNotificationServiceImpl;
use app::app::slack_event_handler::{SlackEventHandlerApp, SlackEventHandlerAppImpl};
use app::app::worker_result_handler::{WorkerResultHandlerApp, WorkerResultHandlerAppImpl};
use infra::infra::config_management::rdb::{
    ConfigManagementRepository, ConfigManagementRepositoryImpl,
};
use infra::infra::cron_scheduler::rdb::CronSchedulerRepositoryImpl;
use infra::infra::jobworkerp_server::rdb::JobworkerpServerRepositoryImpl;
use infra::infra::slack_event_handler::rdb::SlackEventHandlerRepositoryImpl;
use infra::infra::worker_result_handler::rdb::WorkerResultHandlerRepositoryImpl;
use infra::infra::IdGeneratorWrapper;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::test::{setup_test_rdb_from, TEST_RUNTIME};
use jobworkerp_handler::settings::WorkflowSettingFile;
use memory_utils::cache::stretto::{new_memory_cache, MemoryCacheConfig};
use proto::jobworkerp_conductor::data::{ChangeAction, JobworkerpServerData};
use shared::config_events_proto::ConfigChangeEventWrapper;
use shared::notification::service::{ConfigChangeEventReceiver, ConfigChangeNotificationService};

#[cfg(not(feature = "mysql"))]
const MIGRATIONS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../infra/sql/sqlite");
#[cfg(feature = "mysql")]
const MIGRATIONS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../infra/sql/mysql");

static LOCK: Mutex<()> = Mutex::new(());

/// Every kind of setting, with descriptions and a disabled entry of each.
const FULL: &str = r#"
[[jobworkerp]]
name = "main"
address = "http://jobworkerp.example:9000"
description = "primary"

[[jobworkerp]]
name = "spare"
address = "https://spare.example:9443"
enabled = false

[[schedulers]]
name = "nightly"
jobworkerp = "main"
crontab = "0 0 3 * * *"
worker_name = "report"
using = "run"
description = "nightly report"
job_timeout_sec = 120

[[schedulers]]
name = "paused"
jobworkerp = "main"
crontab = "0 0 4 * * *"
workflow_url = "https://example.com/wf.yml"
channel = "batch"
enabled = false

[[listeners]]
name = "follow-up"
jobworkerp = "main"
listen_jobworkerp = "spare"
listen_worker_name = "source"
worker_name = "after"
description = "after source"
enabled = false

[[slack_handlers]]
name = "deploy"
jobworkerp = "main"
message_pattern = "^deploy"
mention_required = true
worker_name = "deployer"
description = "deploy on request"
timeout_sec = 600

[[slack_handlers]]
name = "thumbs"
jobworkerp = "spare"
reaction_names = "+1"
reaction_operation = "added"
workflow_url = "https://example.com/react.yml"
enabled = false
"#;

struct Harness {
    app: ConfigManagementAppImpl,
    servers: Arc<JobworkerpServerAppImpl>,
    schedulers: Arc<CronSchedulerAppImpl>,
    listeners: Arc<WorkerResultHandlerAppImpl>,
    slack_handlers: Arc<SlackEventHandlerAppImpl>,
    notification: Arc<ConfigChangeNotificationServiceImpl>,
}

async fn harness() -> Harness {
    let pool: &'static RdbPool = setup_test_rdb_from(MIGRATIONS).await;
    let repository = ConfigManagementRepositoryImpl::new(pool);
    let mut conn = pool.acquire().await.expect("connection");
    repository
        .delete_all_configs(&mut conn)
        .await
        .expect("start from no settings");
    let mc = MemoryCacheConfig::default();
    let notification = Arc::new(
        ConfigChangeNotificationServiceImpl::new_memory_default().expect("in-memory notification"),
    );
    let notifier: Arc<dyn ConfigChangeNotificationService> = notification.clone();
    let server_repo = JobworkerpServerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
    let servers = Arc::new(JobworkerpServerAppImpl::new(
        server_repo.clone(),
        new_memory_cache(&mc),
        notifier.clone(),
    ));
    let schedulers = Arc::new(CronSchedulerAppImpl::new(
        CronSchedulerRepositoryImpl::new(IdGeneratorWrapper::new(), pool),
        new_memory_cache(&mc),
        notifier.clone(),
    ));
    let listeners = Arc::new(WorkerResultHandlerAppImpl::new(
        WorkerResultHandlerRepositoryImpl::new(IdGeneratorWrapper::new(), pool),
        new_memory_cache(&mc),
        notifier.clone(),
    ));
    let slack_handlers = Arc::new(SlackEventHandlerAppImpl::new(
        SlackEventHandlerRepositoryImpl::new(IdGeneratorWrapper::new(), pool.clone()),
        server_repo,
        new_memory_cache(&mc),
        notifier.clone(),
    ));
    let app = ConfigManagementAppImpl::new(
        repository,
        servers.clone(),
        schedulers.clone(),
        listeners.clone(),
        slack_handlers.clone(),
        notifier,
    );
    Harness {
        app,
        servers,
        schedulers,
        listeners,
        slack_handlers,
        notification,
    }
}

fn run(test: impl std::future::Future<Output = ()>) {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    TEST_RUNTIME.block_on(test);
}

/// Events delivered so far, in order.
async fn drain(receiver: &mut Box<dyn ConfigChangeEventReceiver>) -> Vec<ConfigChangeEventWrapper> {
    let mut events = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_millis(300), receiver.receive()).await
    {
        events.push(event);
    }
    events
}

fn kind(event: &ConfigChangeEventWrapper) -> &'static str {
    if event.is_jobworkerp_server() {
        "server"
    } else if event.is_cron_scheduler() {
        "scheduler"
    } else if event.is_worker_result_handler() {
        "listener"
    } else {
        "slack"
    }
}

fn counts(report: &ImportReport) -> [i32; 4] {
    [
        report.servers,
        report.schedulers,
        report.listeners,
        report.slack_handlers,
    ]
}

async fn import(h: &Harness, toml: &str, overwrite: bool) -> ImportReport {
    h.app
        .import_toml_config(toml, overwrite, false)
        .await
        .expect("import runs")
}

#[test]
fn export_clear_import_restores_every_kind_with_description_and_enabled() {
    run(async {
        let h = harness().await;
        let report = import(&h, FULL, true).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(counts(&report), [2, 2, 1, 2]);

        let export = h.app.export_toml_config(false).await.unwrap();
        assert!(export.left_out.is_empty(), "{:?}", export.left_out);
        let exported = export.toml_content;
        h.app.clear_all_configs().await.unwrap();
        assert_eq!(h.servers.count().await.unwrap(), 0);
        assert_eq!(h.slack_handlers.count().await.unwrap(), 0);

        let again = import(&h, &exported, false).await;
        assert!(again.errors.is_empty(), "{:?}", again.errors);
        assert_eq!(
            h.app.export_toml_config(false).await.unwrap().toml_content,
            exported
        );

        let file = WorkflowSettingFile::parse(&exported).unwrap();
        let spare = file.jobworkerp.iter().find(|s| s.name == "spare").unwrap();
        assert_eq!(spare.enabled, Some(false));
        let main = file.jobworkerp.iter().find(|s| s.name == "main").unwrap();
        assert_eq!(main.description.as_deref(), Some("primary"));
        let slack = file.slack_handlers.unwrap();
        let deploy = slack.iter().find(|s| s.name == "deploy").unwrap();
        assert_eq!(deploy.description.as_deref(), Some("deploy on request"));
        assert_eq!(deploy.timeout_sec, Some(600));
        let thumbs = slack.iter().find(|s| s.name == "thumbs").unwrap();
        assert_eq!(thumbs.enabled, Some(false));
        assert_eq!(thumbs.reaction_operation.as_deref(), Some("added"));
    });
}

/// Deleting a server leaves what ran on it in place; such entries cannot be
/// imported again, so the export names them instead of dropping them unseen.
#[test]
fn export_names_the_entries_it_leaves_out() {
    run(async {
        let h = harness().await;
        import(&h, FULL, true).await;
        let spare = h
            .servers
            .find_jobworkerp_server_by_name("spare", None)
            .await
            .unwrap()
            .unwrap();
        h.servers
            .delete_jobworkerp_server(&spare.id.unwrap())
            .await
            .unwrap();

        let export = h.app.export_toml_config(false).await.unwrap();
        assert_eq!(export.left_out.len(), 2, "{:?}", export.left_out);
        assert!(export.left_out.iter().any(|e| e.contains("follow-up")));
        assert!(export.left_out.iter().any(|e| e.contains("thumbs")));
        let file = WorkflowSettingFile::parse(&export.toml_content).unwrap();
        assert!(file.listeners.is_none());

        h.app.clear_all_configs().await.unwrap();
        let again = import(&h, &export.toml_content, false).await;
        assert!(again.errors.is_empty(), "{:?}", again.errors);
    });
}

#[test]
fn enabled_only_export_keeps_the_servers_its_items_run_on() {
    run(async {
        let h = harness().await;
        import(&h, FULL, true).await;
        let file =
            WorkflowSettingFile::parse(&h.app.export_toml_config(true).await.unwrap().toml_content)
                .unwrap();
        let names = |items: Option<Vec<_>>| {
            items
                .unwrap_or_default()
                .into_iter()
                .map(|i: jobworkerp_handler::settings::WorkflowSettingItem| i.name)
                .collect::<Vec<_>>()
        };
        assert_eq!(names(file.schedulers), ["nightly"]);
        assert!(file.listeners.is_none());
        let slack: Vec<_> = file
            .slack_handlers
            .unwrap()
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(slack, ["deploy"]);
        let servers: Vec<_> = file.jobworkerp.into_iter().map(|s| s.name).collect();
        assert_eq!(servers, ["main"]);
    });
}

#[test]
fn validate_only_writes_nothing_and_still_counts() {
    run(async {
        let h = harness().await;
        let mut receiver = h.notification.subscribe().await.unwrap();
        let report = h.app.import_toml_config(FULL, true, true).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(counts(&report), [2, 2, 1, 2]);
        assert_eq!(h.servers.count().await.unwrap(), 0);
        assert_eq!(h.schedulers.count().await.unwrap(), 0);
        assert!(drain(&mut receiver).await.is_empty());
    });
}

#[test]
fn validate_only_reports_names_already_taken() {
    run(async {
        let h = harness().await;
        import(&h, FULL, true).await;
        let report = h.app.import_toml_config(FULL, false, true).await.unwrap();
        // The same servers are reused; every other entry clashes by name.
        assert_eq!(report.errors.len(), 5, "{:?}", report.errors);
        assert!(report.errors.iter().any(|e| e.contains("nightly")));
        assert!(report.errors.iter().any(|e| e.contains("deploy")));
        // Overwriting would replace them, so nothing clashes then.
        let overwrite = h.app.import_toml_config(FULL, true, true).await.unwrap();
        assert!(overwrite.errors.is_empty(), "{:?}", overwrite.errors);
        assert_eq!(h.schedulers.count().await.unwrap(), 2);
    });
}

#[test]
fn import_collects_every_error_and_writes_nothing() {
    run(async {
        let h = harness().await;
        let broken = r#"
[[jobworkerp]]
name = "main"
address = "not a url"

[[jobworkerp]]
name = "fine"
address = "http://fine.example:9000"

[[schedulers]]
name = "no-crontab"
jobworkerp = "main"
worker_name = "w"

[[schedulers]]
name = "unknown-server"
jobworkerp = "nowhere"
crontab = "0 0 * * * *"
worker_name = "w"

[[listeners]]
name = "listens-nowhere"
jobworkerp = "main"
listen_worker_name = "source"
worker_name = "w"

[[slack_handlers]]
name = "bad-regex"
jobworkerp = "main"
message_pattern = "("
worker_name = "w"
"#;
        let report = h
            .app
            .import_toml_config(broken, false, false)
            .await
            .unwrap();
        assert_eq!(report.errors.len(), 5, "{:?}", report.errors);
        // Nothing was written, so nothing counts as imported.
        assert_eq!(counts(&report), [0, 0, 0, 0]);
        assert!(report.errors.iter().any(|e| e.contains("not a url")));
        assert!(report.errors.iter().any(|e| e.contains("no-crontab")));
        assert!(report.errors.iter().any(|e| e.contains("nowhere")));
        assert!(report.errors.iter().any(|e| e.contains("bad-regex")));
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.contains("listens-nowhere") && e.contains("no listen_jobworkerp")),
            "{:?}",
            report.errors
        );
        assert_eq!(h.servers.count().await.unwrap(), 0);

        let malformed = h.app.import_toml_config("[[x", false, false).await.unwrap();
        assert_eq!(malformed.errors.len(), 1);
    });
}

#[test]
fn import_reuses_a_matching_server_and_rejects_a_different_one() {
    run(async {
        let h = harness().await;
        import(&h, FULL, true).await;
        let more = r#"
[[jobworkerp]]
name = "main"
address = "http://jobworkerp.example:9000"

[[schedulers]]
name = "extra"
jobworkerp = "main"
crontab = "0 0 5 * * *"
worker_name = "w"
"#;
        let report = import(&h, more, false).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(counts(&report), [0, 1, 0, 0]);
        assert_eq!(h.servers.count().await.unwrap(), 2);

        // A file may also leave the stored servers out altogether.
        let without_servers = r#"
[[schedulers]]
name = "no-servers"
jobworkerp = "main"
crontab = "0 0 6 * * *"
worker_name = "w"
"#;
        let report = import(&h, without_servers, false).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(counts(&report), [0, 1, 0, 0]);

        let moved = more.replace("jobworkerp.example:9000", "elsewhere:9000");
        let report = import(&h, &moved.replace("extra", "extra2"), false).await;
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        assert!(report.errors[0].contains("main"));
    });
}

#[test]
fn import_announces_servers_before_what_runs_on_them() {
    run(async {
        let h = harness().await;
        let mut receiver = h.notification.subscribe().await.unwrap();
        import(&h, FULL, true).await;
        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 7);
        assert!(events.iter().all(|e| e.action() == ChangeAction::Created));
        let kinds: Vec<_> = events.iter().map(kind).collect();
        assert_eq!(&kinds[..2], ["server", "server"]);
        let paused = events
            .iter()
            .filter_map(|e| e.as_cron_scheduler())
            .find(|e| e.name == "paused")
            .unwrap();
        assert!(!paused.data.as_ref().unwrap().enabled);
    });
}

#[test]
fn overwrite_and_clear_announce_deletions_dependents_first() {
    run(async {
        let h = harness().await;
        import(&h, FULL, true).await;
        let mut receiver = h.notification.subscribe().await.unwrap();
        h.app.clear_all_configs().await.unwrap();
        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 7);
        assert!(events.iter().all(|e| e.action() == ChangeAction::Deleted));
        let kinds: Vec<_> = events.iter().map(kind).collect();
        assert_eq!(&kinds[5..], ["server", "server"]);
        assert_eq!(h.slack_handlers.count().await.unwrap(), 0);

        import(&h, FULL, true).await;
        let mut receiver = h.notification.subscribe().await.unwrap();
        import(&h, FULL, true).await;
        let events = drain(&mut receiver).await;
        let actions: Vec<_> = events.iter().map(|e| e.action()).collect();
        assert_eq!(actions.len(), 14);
        assert!(actions[..7].iter().all(|a| *a == ChangeAction::Deleted));
        assert!(actions[7..].iter().all(|a| *a == ChangeAction::Created));
    });
}

#[test]
fn overwrite_drops_cached_lookups() {
    run(async {
        let h = harness().await;
        import(&h, FULL, true).await;
        let before = h
            .servers
            .find_jobworkerp_server_by_name("main", None)
            .await
            .unwrap()
            .unwrap();
        let moved = FULL.replace("jobworkerp.example:9000", "moved.example:9000");
        import(&h, &moved, true).await;
        let after = h
            .servers
            .find_jobworkerp_server_by_name("main", None)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(before.id, after.id);
        let data: JobworkerpServerData = after.data.unwrap();
        assert_eq!(data.host, "moved.example");
        assert!(h
            .schedulers
            .find_cron_scheduler_by_name("nightly", None)
            .await
            .unwrap()
            .is_some());
        assert!(h
            .listeners
            .find_worker_result_handler_by_name("follow-up", None)
            .await
            .unwrap()
            .is_some());
    });
}

#[test]
fn slack_handler_without_timeout_gets_the_default() {
    run(async {
        let h = harness().await;
        import(&h, FULL, true).await;
        let file = WorkflowSettingFile::parse(
            &h.app.export_toml_config(false).await.unwrap().toml_content,
        )
        .unwrap();
        let thumbs = file
            .slack_handlers
            .unwrap()
            .into_iter()
            .find(|s| s.name == "thumbs")
            .unwrap();
        assert_eq!(thumbs.timeout_sec, Some(3600));
    });
}

/// SQLite: another connection holding the write lock makes an import wait,
/// not fail with "database is locked" when it moves from reading to writing.
#[cfg(not(feature = "mysql"))]
#[test]
fn import_waits_for_another_writer() {
    run(async {
        let h = harness().await;
        import(&h, FULL, true).await;
        let pool = setup_test_rdb_from(MIGRATIONS).await;
        let mut writer = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("UPDATE jobworkerp_server SET description = 'busy' WHERE name = 'main'")
            .execute(&mut *writer)
            .await
            .unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            writer.commit().await.unwrap();
        });
        let more = r#"
[[jobworkerp]]
name = "main"
address = "http://jobworkerp.example:9000"

[[schedulers]]
name = "while-busy"
jobworkerp = "main"
crontab = "0 0 5 * * *"
worker_name = "w"
"#;
        let report = import(&h, more, false).await;
        release.await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(counts(&report), [0, 1, 0, 0]);
    });
}
