pub mod items;

use crate::app::cron_scheduler::{CronSchedulerApp, CronSchedulerAppImpl};
use crate::app::jobworkerp_server::JobworkerpServerAppImpl;
use crate::app::slack_event_handler::{SlackEventHandlerApp, SlackEventHandlerAppImpl};
use crate::app::worker_result_handler::{WorkerResultHandlerApp, WorkerResultHandlerAppImpl};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use command_utils::util::datetime::now_seconds;
use infra::error::UiEventHandlerError;
use infra::infra::config_management::rdb::{
    ConfigIds, ConfigManagementRepository, ConfigManagementRepositoryImpl, Connection,
    StoredConfigs,
};
use infra::infra::cron_scheduler::rdb::{CronSchedulerRepository, UseCronSchedulerRepository};
use infra::infra::jobworkerp_server::rdb::{
    JobworkerpServerRepository, JobworkerpServerRepositoryImpl, UseJobworkerpServerRepository,
};
use infra::infra::slack_event_handler::rdb::{
    SlackEventHandlerRepository, UseSlackEventHandlerRepository,
};
use infra::infra::worker_result_handler::rdb::{
    UseWorkerResultHandlerRepository, WorkerResultHandlerRepository,
};
use items::{
    listener_data, listener_item, scheduler_data, scheduler_item, server_data, server_item,
    slack_data, slack_item,
};
use jobworkerp_handler::settings::WorkflowSettingFile;
use memory_utils::cache::stretto::UseMemoryCache;
use proto::jobworkerp_conductor::data::{
    CronSchedulerData, CronSchedulerId, JobworkerpServer, JobworkerpServerData, JobworkerpServerId,
    SlackEventHandlerData, SlackEventHandlerId, WorkerResultHandlerData, WorkerResultHandlerId,
};
use shared::config_events_proto::ConfigChangeEventWrapper as Event;
use shared::notification::service::ConfigChangeNotificationService;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// What an import created, or would create, per kind; `errors` lists every
/// entry that could not be imported, and then nothing is written and every
/// count is 0.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ImportReport {
    pub servers: i32,
    pub schedulers: i32,
    pub listeners: i32,
    pub slack_handlers: i32,
    pub errors: Vec<String>,
}

/// An exported settings file. `left_out` names each entry that could not be
/// written to it (one running on a server that no longer exists), so a
/// backup that is not complete says so.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct TomlExport {
    pub toml_content: String,
    pub left_out: Vec<String>,
}

#[async_trait]
pub trait ConfigManagementApp: Send + Sync {
    /// Imports a settings file in one transaction. `overwrite_existing`
    /// deletes every setting first. `validate_only` runs the same import and
    /// rolls it back, so the counts and errors are those of a real run.
    async fn import_toml_config(
        &self,
        toml_content: &str,
        overwrite_existing: bool,
        validate_only: bool,
    ) -> Result<ImportReport>;

    /// The settings as a TOML file. With `enabled_only`, the servers the
    /// exported entries run on come along even when disabled, so the file
    /// imports as it is.
    async fn export_toml_config(&self, enabled_only: bool) -> Result<TomlExport>;

    /// Deletes every setting; execution history stays.
    async fn clear_all_configs(&self) -> Result<()>;
}

pub struct ConfigManagementAppImpl {
    repository: ConfigManagementRepositoryImpl,
    servers: Arc<JobworkerpServerAppImpl>,
    schedulers: Arc<CronSchedulerAppImpl>,
    listeners: Arc<WorkerResultHandlerAppImpl>,
    slack_handlers: Arc<SlackEventHandlerAppImpl>,
    notification_service: Arc<dyn ConfigChangeNotificationService>,
}

/// The rows an import inserted, announced once it commits.
#[derive(Default)]
struct Created {
    servers: Vec<(JobworkerpServerId, JobworkerpServerData)>,
    schedulers: Vec<(CronSchedulerId, CronSchedulerData)>,
    listeners: Vec<(WorkerResultHandlerId, WorkerResultHandlerData)>,
    slack_handlers: Vec<(SlackEventHandlerId, SlackEventHandlerData)>,
}

impl ConfigManagementAppImpl {
    pub fn new(
        repository: ConfigManagementRepositoryImpl,
        servers: Arc<JobworkerpServerAppImpl>,
        schedulers: Arc<CronSchedulerAppImpl>,
        listeners: Arc<WorkerResultHandlerAppImpl>,
        slack_handlers: Arc<SlackEventHandlerAppImpl>,
        notification_service: Arc<dyn ConfigChangeNotificationService>,
    ) -> Self {
        Self {
            repository,
            servers,
            schedulers,
            listeners,
            slack_handlers,
            notification_service,
        }
    }

    /// Inserts the file's entries, servers first so the rest can name them.
    /// An entry that fails is reported and skipped, so one run lists every
    /// problem; the caller rolls back if there were any.
    async fn insert_all(
        &self,
        conn: &mut Connection,
        file: &WorkflowSettingFile,
        overwrite_existing: bool,
        report: &mut ImportReport,
    ) -> Result<Created> {
        // The entry's value, or its problem recorded and the entry skipped.
        macro_rules! entry {
            ($label:expr, $result:expr) => {
                match $result {
                    Ok(value) => value,
                    Err(e) => {
                        report.errors.push(format!("{}: {e:#}", $label));
                        continue;
                    }
                }
            };
        }
        let now = now_seconds();
        let server_repo = self.servers.jobworkerp_server_repository();
        let mut created = Created::default();
        let mut resolver = ServerResolver {
            repository: server_repo,
            ids: HashMap::new(),
            look_up_stored: !overwrite_existing,
        };

        for item in &file.jobworkerp {
            let label = format!("Server '{}'", item.name);
            let data = entry!(label, server_data(item, now));
            // Without overwriting, a file usually carries the servers its
            // entries need; an identical one already stored is reused.
            if !overwrite_existing {
                if let Some(stored) = server_repo.find_by_name_tx(&mut *conn, &item.name).await? {
                    if (&stored.host, &stored.port, stored.ssl_enabled)
                        == (&data.host, &data.port, data.ssl_enabled)
                    {
                        let id = JobworkerpServerId { value: stored.id };
                        resolver.ids.insert(item.name.clone(), id);
                    } else {
                        report
                            .errors
                            .push(format!("{label}: already exists with a different address"));
                    }
                    continue;
                }
            }
            let id = entry!(
                label,
                name_clash(server_repo.create(&mut *conn, &data).await)?
            );
            report.servers += 1;
            resolver.ids.insert(item.name.clone(), id);
            created.servers.push((id, data));
        }

        for item in file.schedulers.iter().flatten() {
            let label = format!("Scheduler '{}'", item.name);
            let mut data = entry!(
                label,
                scheduler_data(item, JobworkerpServerId::default(), now).and_then(|data| {
                    CronSchedulerAppImpl::validate_execution_target(&data).map(|_| data)
                })
            );
            data.jobworkerp_server_id = Some(entry!(
                label,
                resolver.resolve(conn, &item.jobworkerp).await?
            ));
            let repository = self.schedulers.cron_scheduler_repository();
            let id = entry!(
                label,
                name_clash(repository.create(&mut *conn, &data).await)?
            );
            report.schedulers += 1;
            created.schedulers.push((id, data));
        }

        for item in file.listeners.iter().flatten() {
            let label = format!("Listener '{}'", item.name);
            let mut data = entry!(
                label,
                listener_data(item, Default::default(), Default::default(), now).and_then(|data| {
                    WorkerResultHandlerAppImpl::validate_execution_target(&data).map(|_| data)
                })
            );
            let listen_name = item.listen_jobworkerp.as_deref().unwrap_or_default();
            data.listen_jobworkerp_server_id =
                Some(entry!(label, resolver.resolve(conn, listen_name).await?));
            data.process_jobworkerp_server_id = Some(entry!(
                label,
                resolver.resolve(conn, &item.jobworkerp).await?
            ));
            let repository = self.listeners.worker_result_handler_repository();
            let id = entry!(
                label,
                name_clash(repository.create(&mut *conn, &data).await)?
            );
            report.listeners += 1;
            created.listeners.push((id, data));
        }

        for item in file.slack_handlers.iter().flatten() {
            let label = format!("Slack handler '{}'", item.name);
            let mut data = entry!(
                label,
                slack_data(item, JobworkerpServerId::default(), now).and_then(|data| {
                    SlackEventHandlerAppImpl::validate_handler_data(&data).map(|_| data)
                })
            );
            data.jobworkerp_server_id = Some(entry!(
                label,
                resolver.resolve(conn, &item.jobworkerp).await?
            ));
            let repository = self.slack_handlers.slack_event_handler_repository();
            let id = entry!(
                label,
                name_clash(repository.create(&mut *conn, &data).await)?
            );
            report.slack_handlers += 1;
            created.slack_handlers.push((id, data));
        }
        Ok(created)
    }

    /// Tells the running handlers what was deleted: what runs on a server
    /// before the server itself.
    async fn announce_deleted(&self, ids: ConfigIds) {
        let listeners = ids.listeners.into_iter().map(|(id, name)| {
            Event::create_worker_result_handler_deleted(
                name,
                Some(WorkerResultHandlerId { value: id }),
                None,
                HashMap::new(),
            )
        });
        let schedulers = ids.schedulers.into_iter().map(|(id, name)| {
            Event::create_cron_scheduler_deleted(name, Some(CronSchedulerId { value: id }))
        });
        let slack_handlers = ids.slack_handlers.into_iter().map(|(id, name)| {
            Event::create_slack_event_handler_deleted(name, Some(SlackEventHandlerId { value: id }))
        });
        let servers = ids.servers.into_iter().map(|(id, name)| {
            Event::create_jobworkerp_server_deleted(name, JobworkerpServerId { value: id }, None)
        });
        let events: Vec<_> = listeners
            .chain(schedulers)
            .chain(slack_handlers)
            .chain(servers)
            .collect();
        for event in events {
            self.notify(event).await;
        }
    }

    /// Tells the running handlers what was created, servers first: a
    /// scheduler or listener starting up looks its server up by then. The
    /// import has committed, so nothing here may turn it into a failure.
    async fn announce_created(&self, created: Created) {
        let mut servers: HashMap<i64, Option<JobworkerpServer>> = HashMap::new();
        for (id, data) in created.servers {
            let server = JobworkerpServer {
                id: Some(id),
                data: Some(data.clone()),
            };
            servers.insert(id.value, Some(server));
            let event = Event::create_jobworkerp_server_created(data.name.clone(), id, Some(data));
            self.notify(event).await;
        }
        for (id, data) in created.schedulers {
            let event =
                Event::create_cron_scheduler_created(data.name.clone(), Some(id), Some(data), None);
            self.notify(event).await;
        }
        for (id, data) in created.listeners {
            let event = Event::create_worker_result_handler_created(
                data.name.clone(),
                Some(id),
                Some(data),
                HashMap::new(),
            );
            self.notify(event).await;
        }
        for (id, data) in created.slack_handlers {
            // Slack events carry their server, which may be one reused from
            // before the import.
            let server_id = data.jobworkerp_server_id.unwrap_or_default();
            let server = match servers.get(&server_id.value) {
                Some(server) => server.clone(),
                None => {
                    let repository = self.servers.jobworkerp_server_repository();
                    let found = repository.find(&server_id).await.unwrap_or_else(|e| {
                        tracing::warn!("Slack handler notified without its server: {e:#}");
                        None
                    });
                    servers.insert(server_id.value, found.clone());
                    found
                }
            };
            let event = Event::create_slack_event_handler_created(
                data.name.clone(),
                Some(id),
                Some(data),
                server,
            );
            self.notify(event).await;
        }
    }

    async fn notify(&self, event: Event) {
        if let Err(e) = self.notification_service.notify(event).await {
            tracing::warn!("Failed to send a config change notification: {e:#}");
        }
    }

    /// The per-entity apps cache lookups by id and name; a bulk write
    /// bypasses them, so their caches would answer with what is gone.
    async fn clear_caches(&self) {
        let results = [
            self.servers.clear().await,
            self.schedulers.clear().await,
            self.listeners.clear().await,
            self.slack_handlers.clear().await,
        ];
        for e in results.into_iter().filter_map(Result::err) {
            tracing::warn!("Failed to clear a config cache: {e:#}");
        }
    }
}

/// A name already taken is the entry's problem; any other failure to insert
/// is the database's, and ends the import.
fn name_clash<T>(result: Result<T>) -> Result<Result<T>> {
    match result {
        Err(e)
            if matches!(
                e.downcast_ref(),
                Some(UiEventHandlerError::AlreadyExists(_))
            ) =>
        {
            Ok(Err(e))
        }
        other => other.map(Ok),
    }
}

/// Server names to ids: the file's servers first, then (when not
/// overwriting) those already stored.
struct ServerResolver<'a> {
    repository: &'a JobworkerpServerRepositoryImpl,
    ids: HashMap<String, JobworkerpServerId>,
    look_up_stored: bool,
}

impl ServerResolver<'_> {
    /// The outer error is a database failure; the inner one is the entry's
    /// own problem, a name that leads nowhere.
    async fn resolve(
        &mut self,
        conn: &mut Connection,
        name: &str,
    ) -> Result<Result<JobworkerpServerId>> {
        if let Some(id) = self.ids.get(name) {
            return Ok(Ok(*id));
        }
        if self.look_up_stored {
            if let Some(row) = self.repository.find_by_name_tx(&mut *conn, name).await? {
                let id = JobworkerpServerId { value: row.id };
                self.ids.insert(name.to_string(), id);
                return Ok(Ok(id));
            }
        }
        Ok(Err(anyhow!("jobworkerp server '{name}' is not defined")))
    }
}

/// The stored settings as a settings file, and the entries left out of it;
/// see `export_toml_config`.
fn settings_file(stored: StoredConfigs, enabled_only: bool) -> (WorkflowSettingFile, Vec<String>) {
    let names: HashMap<i64, String> = stored
        .servers
        .iter()
        .map(|(id, data)| (*id, data.name.clone()))
        .collect();
    let name_of = |id: i64| names.get(&id).cloned();
    let mut left_out = Vec::new();
    // An entry naming a server that no longer exists cannot be imported
    // again, so it is left out and named in `left_out`.
    fn kept<D, I>(
        entries: &[D],
        enabled_only: bool,
        left_out: &mut Vec<String>,
        enabled: impl Fn(&D) -> bool,
        to_item: impl Fn(&D) -> Result<I>,
    ) -> Vec<I> {
        entries
            .iter()
            .filter(|data| enabled(data) || !enabled_only)
            .filter_map(|data| match to_item(data) {
                Ok(item) => Some(item),
                Err(e) => {
                    let entry = format!("{e:#}");
                    tracing::warn!("Not exported: {entry}");
                    left_out.push(entry);
                    None
                }
            })
            .collect()
    }
    let schedulers = kept(
        &stored.schedulers,
        enabled_only,
        &mut left_out,
        |d| d.enabled,
        |d| scheduler_item(d, &name_of).with_context(|| format!("scheduler '{}'", d.name)),
    );
    let listeners = kept(
        &stored.listeners,
        enabled_only,
        &mut left_out,
        |d| d.enabled,
        |d| listener_item(d, &name_of).with_context(|| format!("result handler '{}'", d.name)),
    );
    let slack_handlers = kept(
        &stored.slack_handlers,
        enabled_only,
        &mut left_out,
        |d| d.enabled,
        |d| slack_item(d, &name_of).with_context(|| format!("Slack handler '{}'", d.name)),
    );

    let referenced: BTreeSet<&str> = schedulers
        .iter()
        .chain(&listeners)
        .flat_map(|i| [Some(i.jobworkerp.as_str()), i.listen_jobworkerp.as_deref()])
        .chain(slack_handlers.iter().map(|i| Some(i.jobworkerp.as_str())))
        .flatten()
        .collect();
    let servers = stored
        .servers
        .iter()
        .map(|(_, data)| data)
        .filter(|data| data.enabled || !enabled_only || referenced.contains(data.name.as_str()))
        .map(server_item)
        .collect();

    fn non_empty<T>(items: Vec<T>) -> Option<Vec<T>> {
        (!items.is_empty()).then_some(items)
    }
    let file = WorkflowSettingFile {
        jobworkerp: servers,
        schedulers: non_empty(schedulers),
        listeners: non_empty(listeners),
        slack_handlers: non_empty(slack_handlers),
    };
    (file, left_out)
}

#[async_trait]
impl ConfigManagementApp for ConfigManagementAppImpl {
    async fn import_toml_config(
        &self,
        toml_content: &str,
        overwrite_existing: bool,
        validate_only: bool,
    ) -> Result<ImportReport> {
        let file = match WorkflowSettingFile::parse(toml_content) {
            Ok(file) => file,
            Err(e) => {
                return Ok(ImportReport {
                    errors: vec![format!("{e:#}")],
                    ..Default::default()
                })
            }
        };
        let mut tx = self.repository.begin_bulk_write().await?;
        let deleted = if overwrite_existing {
            let ids = self.repository.list_config_ids(&mut tx).await?;
            self.repository.delete_all_configs(&mut tx).await?;
            Some(ids)
        } else {
            None
        };
        let mut report = ImportReport::default();
        let created = self
            .insert_all(&mut tx, &file, overwrite_existing, &mut report)
            .await?;

        if validate_only || !report.errors.is_empty() {
            tx.rollback().await.map_err(UiEventHandlerError::DBError)?;
            if !report.errors.is_empty() {
                report = ImportReport {
                    errors: report.errors,
                    ..Default::default()
                };
            }
            return Ok(report);
        }
        tx.commit().await.map_err(UiEventHandlerError::DBError)?;
        self.clear_caches().await;
        if let Some(ids) = deleted {
            self.announce_deleted(ids).await;
        }
        self.announce_created(created).await;
        Ok(report)
    }

    async fn export_toml_config(&self, enabled_only: bool) -> Result<TomlExport> {
        let stored = self.repository.load_all_configs().await?;
        let (file, left_out) = settings_file(stored, enabled_only);
        let toml_content = toml::to_string_pretty(&file).context("Failed to serialize to TOML")?;
        Ok(TomlExport {
            toml_content,
            left_out,
        })
    }

    async fn clear_all_configs(&self) -> Result<()> {
        let mut tx = self.repository.begin_bulk_write().await?;
        let ids = self.repository.list_config_ids(&mut tx).await?;
        self.repository.delete_all_configs(&mut tx).await?;
        tx.commit().await.map_err(UiEventHandlerError::DBError)?;
        self.clear_caches().await;
        self.announce_deleted(ids).await;
        Ok(())
    }
}
