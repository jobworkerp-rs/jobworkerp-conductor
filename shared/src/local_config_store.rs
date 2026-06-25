use anyhow::Result;
use proto::jobworkerp_conductor::data::{
    CronScheduler, CronSchedulerId, JobworkerpServer, JobworkerpServerId, SlackEventHandler,
    SlackEventHandlerId, WorkerResultHandler, WorkerResultHandlerId,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

/// Configuration snapshot for versioning and rollback
#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    pub version: u64,
    pub timestamp: SystemTime,
    pub cron_schedulers: HashMap<CronSchedulerId, CronScheduler>,
    pub worker_result_handlers: HashMap<WorkerResultHandlerId, WorkerResultHandler>,
    pub jobworkerp_servers: HashMap<JobworkerpServerId, JobworkerpServer>,
    pub slack_event_handlers: HashMap<SlackEventHandlerId, SlackEventHandler>,
    pub description: String,
}

/// Configuration change types for diff tracking
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigDiff {
    CronSchedulerAdded(CronSchedulerId),
    CronSchedulerUpdated(CronSchedulerId),
    CronSchedulerRemoved(CronSchedulerId),
    WorkerResultHandlerAdded(WorkerResultHandlerId),
    WorkerResultHandlerUpdated(WorkerResultHandlerId),
    WorkerResultHandlerRemoved(WorkerResultHandlerId),
    JobworkerpServerAdded(JobworkerpServerId),
    JobworkerpServerUpdated(JobworkerpServerId),
    JobworkerpServerRemoved(JobworkerpServerId),
    SlackEventHandlerAdded(SlackEventHandlerId),
    SlackEventHandlerUpdated(SlackEventHandlerId),
    SlackEventHandlerRemoved(SlackEventHandlerId),
}

/// Configuration export format for backup/restore
#[derive(Debug, Clone)]
pub struct ConfigExport {
    pub version: u64,
    pub timestamp: SystemTime,
    pub cron_schedulers: HashMap<CronSchedulerId, CronScheduler>,
    pub worker_result_handlers: HashMap<WorkerResultHandlerId, WorkerResultHandler>,
    pub jobworkerp_servers: HashMap<JobworkerpServerId, JobworkerpServer>,
    pub slack_event_handlers: HashMap<SlackEventHandlerId, SlackEventHandler>,
}

/// LocalConfigStore: 全ハンドラーが共有する設定ストア
/// API ServerとEvent Handler Server間で同期すべき設定の集約
#[derive(Debug, Clone)]
pub struct LocalConfigStore {
    // ID-based primary storage
    cron_schedulers: HashMap<CronSchedulerId, CronScheduler>,
    worker_result_handlers: HashMap<WorkerResultHandlerId, WorkerResultHandler>,
    jobworkerp_servers: HashMap<JobworkerpServerId, JobworkerpServer>,
    slack_event_handlers: HashMap<SlackEventHandlerId, SlackEventHandler>,

    // Reverse index for name-based lookups
    cron_scheduler_name_index: HashMap<String, CronSchedulerId>,
    worker_handler_name_index: HashMap<String, WorkerResultHandlerId>,
    jobworkerp_server_name_index: HashMap<String, JobworkerpServerId>,
    slack_event_handler_name_index: HashMap<String, SlackEventHandlerId>,

    last_sync: SystemTime,
    version: u64,                   // Configuration version for tracking changes
    snapshots: Vec<ConfigSnapshot>, // History of configuration snapshots
}

impl LocalConfigStore {
    pub fn new() -> Self {
        Self {
            cron_schedulers: HashMap::new(),
            worker_result_handlers: HashMap::new(),
            jobworkerp_servers: HashMap::new(),
            slack_event_handlers: HashMap::new(),
            cron_scheduler_name_index: HashMap::new(),
            worker_handler_name_index: HashMap::new(),
            jobworkerp_server_name_index: HashMap::new(),
            slack_event_handler_name_index: HashMap::new(),
            last_sync: SystemTime::now(),
            version: 1,
            snapshots: Vec::new(),
        }
    }

    // ID-based API - CronScheduler
    pub fn get_cron_scheduler(&self, id: &CronSchedulerId) -> Option<&CronScheduler> {
        self.cron_schedulers.get(id)
    }

    pub fn upsert_cron_scheduler(&mut self, scheduler: CronScheduler) -> Result<()> {
        if let (Some(id), Some(data)) = (scheduler.id.as_ref(), &scheduler.data) {
            let id_copy = *id;
            let name = data.name.clone();

            // Remove old name index if name changed
            if let Some(old_scheduler) = self.cron_schedulers.get(&id_copy) {
                if let Some(old_data) = &old_scheduler.data {
                    if old_data.name != name {
                        self.cron_scheduler_name_index.remove(&old_data.name);
                        tracing::debug!(
                            "CronScheduler name changed: '{}' -> '{}' (id={})",
                            old_data.name,
                            name,
                            id_copy.value
                        );
                    }
                }
            }

            self.cron_schedulers.insert(id_copy, scheduler);
            self.cron_scheduler_name_index.insert(name.clone(), id_copy);

            tracing::debug!(
                "Upserted cron_scheduler: id={}, name={}",
                id_copy.value,
                name
            );
            self.update_sync_time();
            Ok(())
        } else {
            Err(anyhow::anyhow!("CronScheduler id or data is missing"))
        }
    }

    pub fn remove_cron_scheduler(&mut self, id: &CronSchedulerId) -> Option<CronScheduler> {
        let removed = self.cron_schedulers.remove(id);

        if let Some(scheduler) = &removed {
            if let Some(data) = &scheduler.data {
                self.cron_scheduler_name_index.remove(&data.name);
                tracing::debug!(
                    "Removed cron_scheduler: id={}, name={}",
                    id.value,
                    data.name
                );
            }
        }

        if removed.is_some() {
            self.update_sync_time();
        }

        removed
    }

    pub fn get_all_cron_schedulers(&self) -> Vec<&CronScheduler> {
        self.cron_schedulers.values().collect()
    }

    // ID-based API - WorkerResultHandler
    pub fn get_worker_result_handler(
        &self,
        id: &WorkerResultHandlerId,
    ) -> Option<&WorkerResultHandler> {
        self.worker_result_handlers.get(id)
    }

    pub fn upsert_worker_result_handler(&mut self, handler: WorkerResultHandler) -> Result<()> {
        if let (Some(id), Some(data)) = (handler.id.as_ref(), &handler.data) {
            let id_copy = *id;
            let name = data.name.clone();

            if let Some(old_handler) = self.worker_result_handlers.get(&id_copy) {
                if let Some(old_data) = &old_handler.data {
                    if old_data.name != name {
                        self.worker_handler_name_index.remove(&old_data.name);
                        tracing::debug!(
                            "WorkerResultHandler name changed: '{}' -> '{}' (id={})",
                            old_data.name,
                            name,
                            id_copy.value
                        );
                    }
                }
            }

            self.worker_result_handlers.insert(id_copy, handler);
            self.worker_handler_name_index.insert(name.clone(), id_copy);

            tracing::debug!(
                "Upserted worker_result_handler: id={}, name={}",
                id_copy.value,
                name
            );
            self.update_sync_time();
            Ok(())
        } else {
            Err(anyhow::anyhow!("WorkerResultHandler id or data is missing"))
        }
    }

    pub fn remove_worker_result_handler(
        &mut self,
        id: &WorkerResultHandlerId,
    ) -> Option<WorkerResultHandler> {
        let removed = self.worker_result_handlers.remove(id);

        if let Some(handler) = &removed {
            if let Some(data) = &handler.data {
                self.worker_handler_name_index.remove(&data.name);
                tracing::debug!(
                    "Removed worker_result_handler: id={}, name={}",
                    id.value,
                    data.name
                );
            }
        }

        if removed.is_some() {
            self.update_sync_time();
        }

        removed
    }

    pub fn get_all_worker_result_handlers(&self) -> Vec<&WorkerResultHandler> {
        self.worker_result_handlers.values().collect()
    }

    // ID-based API - JobworkerpServer
    pub fn get_jobworkerp_server(&self, id: &JobworkerpServerId) -> Option<&JobworkerpServer> {
        self.jobworkerp_servers.get(id)
    }

    pub fn upsert_jobworkerp_server(&mut self, server: JobworkerpServer) -> Result<()> {
        if let (Some(id), Some(data)) = (server.id.as_ref(), &server.data) {
            let id_copy = *id;
            let name = data.name.clone();

            if let Some(old_server) = self.jobworkerp_servers.get(&id_copy) {
                if let Some(old_data) = &old_server.data {
                    if old_data.name != name {
                        self.jobworkerp_server_name_index.remove(&old_data.name);
                        tracing::debug!(
                            "JobworkerpServer name changed: '{}' -> '{}' (id={})",
                            old_data.name,
                            name,
                            id_copy.value
                        );
                    }
                }
            }

            self.jobworkerp_servers.insert(id_copy, server);
            self.jobworkerp_server_name_index
                .insert(name.clone(), id_copy);

            tracing::debug!(
                "Upserted jobworkerp_server: id={}, name={}",
                id_copy.value,
                name
            );
            self.update_sync_time();
            Ok(())
        } else {
            Err(anyhow::anyhow!("JobworkerpServer id or data is missing"))
        }
    }

    pub fn remove_jobworkerp_server(
        &mut self,
        id: &JobworkerpServerId,
    ) -> Option<JobworkerpServer> {
        let removed = self.jobworkerp_servers.remove(id);

        if let Some(server) = &removed {
            if let Some(data) = &server.data {
                self.jobworkerp_server_name_index.remove(&data.name);
                tracing::debug!(
                    "Removed jobworkerp_server: id={}, name={}",
                    id.value,
                    data.name
                );
            }
        }

        if removed.is_some() {
            self.update_sync_time();
        }

        removed
    }

    pub fn get_all_jobworkerp_servers(&self) -> Vec<&JobworkerpServer> {
        self.jobworkerp_servers.values().collect()
    }

    // ID-based API - SlackEventHandler
    pub fn get_slack_event_handler(&self, id: &SlackEventHandlerId) -> Option<&SlackEventHandler> {
        self.slack_event_handlers.get(id)
    }

    pub fn upsert_slack_event_handler(&mut self, handler: SlackEventHandler) -> Result<()> {
        if let (Some(id), Some(data)) = (handler.id.as_ref(), &handler.data) {
            let id_copy = *id;
            let name = data.name.clone();

            if let Some(old_handler) = self.slack_event_handlers.get(&id_copy) {
                if let Some(old_data) = &old_handler.data {
                    if old_data.name != name {
                        self.slack_event_handler_name_index.remove(&old_data.name);
                        tracing::debug!(
                            "SlackEventHandler name changed: '{}' -> '{}' (id={})",
                            old_data.name,
                            name,
                            id_copy.value
                        );
                    }
                }
            }

            self.slack_event_handlers.insert(id_copy, handler);
            self.slack_event_handler_name_index
                .insert(name.clone(), id_copy);

            tracing::debug!(
                "Upserted slack_event_handler: id={}, name={}",
                id_copy.value,
                name
            );
            self.update_sync_time();
            Ok(())
        } else {
            Err(anyhow::anyhow!("SlackEventHandler id or data is missing"))
        }
    }

    pub fn remove_slack_event_handler(
        &mut self,
        id: &SlackEventHandlerId,
    ) -> Option<SlackEventHandler> {
        let removed = self.slack_event_handlers.remove(id);

        if let Some(handler) = &removed {
            if let Some(data) = &handler.data {
                self.slack_event_handler_name_index.remove(&data.name);
                tracing::debug!(
                    "Removed slack_event_handler: id={}, name={}",
                    id.value,
                    data.name
                );
            }
        }

        if removed.is_some() {
            self.update_sync_time();
        }

        removed
    }

    pub fn get_all_slack_event_handlers(&self) -> Vec<&SlackEventHandler> {
        self.slack_event_handlers.values().collect()
    }

    pub fn find_slack_event_handler_by_name(&self, name: &str) -> Option<&SlackEventHandler> {
        self.slack_event_handler_name_index
            .get(name)
            .and_then(|id| self.slack_event_handlers.get(id))
    }

    pub fn get_slack_event_handler_id_by_name(&self, name: &str) -> Option<SlackEventHandlerId> {
        self.slack_event_handler_name_index.get(name).copied()
    }

    pub fn get_enabled_slack_event_handlers(&self) -> Vec<&SlackEventHandler> {
        self.slack_event_handlers
            .values()
            .filter(|h| h.data.as_ref().map(|d| d.enabled).unwrap_or(false))
            .collect()
    }

    // Stats and metadata
    pub fn get_stats(&self) -> LocalConfigStats {
        LocalConfigStats {
            cron_scheduler_count: self.cron_schedulers.len(),
            worker_result_handler_count: self.worker_result_handlers.len(),
            jobworkerp_server_count: self.jobworkerp_servers.len(),
            slack_event_handler_count: self.slack_event_handlers.len(),
            last_sync: self.last_sync,
            version: self.version,
            snapshot_count: self.snapshots.len(),
        }
    }

    pub fn update_sync_time(&mut self) {
        self.last_sync = SystemTime::now();
        self.version += 1;
    }

    // Reverse lookup API (name -> entity)
    pub fn find_cron_scheduler_by_name(&self, name: &str) -> Option<&CronScheduler> {
        self.cron_scheduler_name_index
            .get(name)
            .and_then(|id| self.cron_schedulers.get(id))
    }

    pub fn get_cron_scheduler_id_by_name(&self, name: &str) -> Option<CronSchedulerId> {
        self.cron_scheduler_name_index.get(name).copied()
    }

    pub fn find_worker_result_handler_by_name(&self, name: &str) -> Option<&WorkerResultHandler> {
        self.worker_handler_name_index
            .get(name)
            .and_then(|id| self.worker_result_handlers.get(id))
    }

    pub fn get_worker_result_handler_id_by_name(
        &self,
        name: &str,
    ) -> Option<WorkerResultHandlerId> {
        self.worker_handler_name_index.get(name).copied()
    }

    pub fn find_jobworkerp_server_by_name(&self, name: &str) -> Option<&JobworkerpServer> {
        self.jobworkerp_server_name_index
            .get(name)
            .and_then(|id| self.jobworkerp_servers.get(id))
    }

    pub fn get_jobworkerp_server_id_by_name(&self, name: &str) -> Option<JobworkerpServerId> {
        self.jobworkerp_server_name_index.get(name).copied()
    }

    /// Create a snapshot of the current configuration
    pub fn create_snapshot(&mut self, description: String) {
        let snapshot = ConfigSnapshot {
            version: self.version,
            timestamp: SystemTime::now(),
            cron_schedulers: self.cron_schedulers.clone(),
            worker_result_handlers: self.worker_result_handlers.clone(),
            jobworkerp_servers: self.jobworkerp_servers.clone(),
            slack_event_handlers: self.slack_event_handlers.clone(),
            description,
        };

        self.snapshots.push(snapshot);

        // Keep only the last 10 snapshots to prevent memory growth
        if self.snapshots.len() > 10 {
            self.snapshots.remove(0);
        }

        tracing::debug!("Created configuration snapshot version {}", self.version);
    }

    /// Restore configuration from a snapshot
    pub fn restore_from_snapshot(&mut self, snapshot_version: u64) -> Result<()> {
        let snapshot = self
            .snapshots
            .iter()
            .find(|s| s.version == snapshot_version)
            .ok_or_else(|| anyhow::anyhow!("Snapshot version {} not found", snapshot_version))?;

        self.cron_schedulers = snapshot.cron_schedulers.clone();
        self.worker_result_handlers = snapshot.worker_result_handlers.clone();
        self.jobworkerp_servers = snapshot.jobworkerp_servers.clone();
        self.slack_event_handlers = snapshot.slack_event_handlers.clone();
        let version = snapshot.version;

        self.rebuild_reverse_indices();

        self.version = version;
        self.last_sync = SystemTime::now();

        tracing::info!(
            "Restored configuration from snapshot version {}",
            snapshot_version
        );
        Ok(())
    }

    /// Rebuild reverse indices from current data
    fn rebuild_reverse_indices(&mut self) {
        self.cron_scheduler_name_index.clear();
        self.worker_handler_name_index.clear();
        self.jobworkerp_server_name_index.clear();
        self.slack_event_handler_name_index.clear();

        for (id, scheduler) in &self.cron_schedulers {
            if let Some(data) = &scheduler.data {
                self.cron_scheduler_name_index
                    .insert(data.name.clone(), *id);
            }
        }

        for (id, handler) in &self.worker_result_handlers {
            if let Some(data) = &handler.data {
                self.worker_handler_name_index
                    .insert(data.name.clone(), *id);
            }
        }

        for (id, server) in &self.jobworkerp_servers {
            if let Some(data) = &server.data {
                self.jobworkerp_server_name_index
                    .insert(data.name.clone(), *id);
            }
        }

        for (id, slack_handler) in &self.slack_event_handlers {
            if let Some(data) = &slack_handler.data {
                self.slack_event_handler_name_index
                    .insert(data.name.clone(), *id);
            }
        }

        tracing::debug!("Rebuilt reverse indices");
    }

    /// Get all available snapshots
    pub fn get_snapshots(&self) -> &[ConfigSnapshot] {
        &self.snapshots
    }

    /// Generate diff between current config and a snapshot
    pub fn diff_with_snapshot(&self, snapshot_version: u64) -> Result<Vec<ConfigDiff>> {
        let snapshot = self
            .snapshots
            .iter()
            .find(|s| s.version == snapshot_version)
            .ok_or_else(|| anyhow::anyhow!("Snapshot version {} not found", snapshot_version))?;

        let mut diffs = Vec::new();

        // Check CronScheduler differences
        for id in self.cron_schedulers.keys() {
            if !snapshot.cron_schedulers.contains_key(id) {
                diffs.push(ConfigDiff::CronSchedulerAdded(*id));
            } else {
                diffs.push(ConfigDiff::CronSchedulerUpdated(*id));
            }
        }

        for id in snapshot.cron_schedulers.keys() {
            if !self.cron_schedulers.contains_key(id) {
                diffs.push(ConfigDiff::CronSchedulerRemoved(*id));
            }
        }

        // Check WorkerResultHandler differences
        for id in self.worker_result_handlers.keys() {
            if !snapshot.worker_result_handlers.contains_key(id) {
                diffs.push(ConfigDiff::WorkerResultHandlerAdded(*id));
            } else {
                diffs.push(ConfigDiff::WorkerResultHandlerUpdated(*id));
            }
        }

        for id in snapshot.worker_result_handlers.keys() {
            if !self.worker_result_handlers.contains_key(id) {
                diffs.push(ConfigDiff::WorkerResultHandlerRemoved(*id));
            }
        }

        // Check JobworkerpServer differences
        for id in self.jobworkerp_servers.keys() {
            if !snapshot.jobworkerp_servers.contains_key(id) {
                diffs.push(ConfigDiff::JobworkerpServerAdded(*id));
            } else {
                diffs.push(ConfigDiff::JobworkerpServerUpdated(*id));
            }
        }

        for id in snapshot.jobworkerp_servers.keys() {
            if !self.jobworkerp_servers.contains_key(id) {
                diffs.push(ConfigDiff::JobworkerpServerRemoved(*id));
            }
        }

        // Check SlackEventHandler differences
        for id in self.slack_event_handlers.keys() {
            if !snapshot.slack_event_handlers.contains_key(id) {
                diffs.push(ConfigDiff::SlackEventHandlerAdded(*id));
            } else {
                diffs.push(ConfigDiff::SlackEventHandlerUpdated(*id));
            }
        }

        for id in snapshot.slack_event_handlers.keys() {
            if !self.slack_event_handlers.contains_key(id) {
                diffs.push(ConfigDiff::SlackEventHandlerRemoved(*id));
            }
        }

        Ok(diffs)
    }

    /// Get current version
    pub fn get_version(&self) -> u64 {
        self.version
    }

    /// Export configuration as a portable format (for backup)
    pub fn export_config(&self) -> ConfigExport {
        ConfigExport {
            version: self.version,
            timestamp: self.last_sync,
            cron_schedulers: self.cron_schedulers.clone(),
            worker_result_handlers: self.worker_result_handlers.clone(),
            jobworkerp_servers: self.jobworkerp_servers.clone(),
            slack_event_handlers: self.slack_event_handlers.clone(),
        }
    }

    /// Import configuration from exported data
    pub fn import_config(&mut self, export: ConfigExport) -> Result<()> {
        self.create_snapshot("Before import".to_string());

        self.cron_schedulers = export.cron_schedulers;
        self.worker_result_handlers = export.worker_result_handlers;
        self.jobworkerp_servers = export.jobworkerp_servers;
        self.slack_event_handlers = export.slack_event_handlers;

        self.rebuild_reverse_indices();

        self.version = export.version;
        self.last_sync = SystemTime::now();

        tracing::info!("Imported configuration version {}", export.version);
        Ok(())
    }

    /// Validate consistency of foreign key references (for testing)
    pub fn validate_consistency(&self) -> Result<()> {
        for scheduler in self.cron_schedulers.values() {
            if let Some(data) = &scheduler.data {
                let server_id = data.jobworkerp_server_id.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "CronScheduler '{}' has missing jobworkerp_server_id",
                        data.name
                    )
                })?;
                let server_exists = self
                    .jobworkerp_servers
                    .values()
                    .any(|s| s.id.as_ref() == Some(server_id));
                if !server_exists {
                    return Err(anyhow::anyhow!(
                        "CronScheduler '{}' references non-existent JobworkerpServer ID: {}",
                        data.name,
                        server_id.value
                    ));
                }
            }
        }

        for handler in self.worker_result_handlers.values() {
            if let Some(data) = &handler.data {
                let listen_server_id =
                    data.listen_jobworkerp_server_id.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "WorkerResultHandler '{}' has missing listen_jobworkerp_server_id",
                            data.name
                        )
                    })?;
                let listen_server_exists = self
                    .jobworkerp_servers
                    .values()
                    .any(|s| s.id.as_ref() == Some(listen_server_id));
                if !listen_server_exists {
                    return Err(anyhow::anyhow!(
                        "WorkerResultHandler '{}' references non-existent listen JobworkerpServer ID: {}",
                        data.name,
                        listen_server_id.value
                    ));
                }

                let process_server_id =
                    data.process_jobworkerp_server_id.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "WorkerResultHandler '{}' has missing process_jobworkerp_server_id",
                            data.name
                        )
                    })?;
                let process_server_exists = self
                    .jobworkerp_servers
                    .values()
                    .any(|s| s.id.as_ref() == Some(process_server_id));
                if !process_server_exists {
                    return Err(anyhow::anyhow!(
                        "WorkerResultHandler '{}' references non-existent process JobworkerpServer ID: {}",
                        data.name,
                        process_server_id.value
                    ));
                }
            }
        }

        for slack_handler in self.slack_event_handlers.values() {
            if let Some(data) = &slack_handler.data {
                let server_id = data.jobworkerp_server_id.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "SlackEventHandler '{}' has missing jobworkerp_server_id",
                        data.name
                    )
                })?;
                let server_exists = self
                    .jobworkerp_servers
                    .values()
                    .any(|s| s.id.as_ref() == Some(server_id));
                if !server_exists {
                    return Err(anyhow::anyhow!(
                        "SlackEventHandler '{}' references non-existent JobworkerpServer ID: {}",
                        data.name,
                        server_id.value
                    ));
                }
            }
        }

        tracing::debug!("Local configuration consistency validated successfully");
        Ok(())
    }

    /// Validate forward/reverse index consistency
    pub fn validate_index_consistency(&self) -> Result<()> {
        // Check CronScheduler indices
        for (id, scheduler) in &self.cron_schedulers {
            if let Some(data) = &scheduler.data {
                let reverse_id = self.cron_scheduler_name_index.get(&data.name);
                if reverse_id != Some(id) {
                    return Err(anyhow::anyhow!(
                        "Index inconsistency: cron_scheduler id={}, name={} (reverse_id={:?})",
                        id.value,
                        data.name,
                        reverse_id
                    ));
                }
            }
        }

        for (name, id) in &self.cron_scheduler_name_index {
            if !self.cron_schedulers.contains_key(id) {
                return Err(anyhow::anyhow!(
                    "Orphaned index entry: cron_scheduler name={}, id={} (no scheduler found)",
                    name,
                    id.value
                ));
            }
        }

        // Check WorkerResultHandler indices
        for (id, handler) in &self.worker_result_handlers {
            if let Some(data) = &handler.data {
                let reverse_id = self.worker_handler_name_index.get(&data.name);
                if reverse_id != Some(id) {
                    return Err(anyhow::anyhow!(
                        "Index inconsistency: worker_result_handler id={}, name={} (reverse_id={:?})",
                        id.value,
                        data.name,
                        reverse_id
                    ));
                }
            }
        }

        for (name, id) in &self.worker_handler_name_index {
            if !self.worker_result_handlers.contains_key(id) {
                return Err(anyhow::anyhow!(
                    "Orphaned index entry: worker_result_handler name={}, id={} (no handler found)",
                    name,
                    id.value
                ));
            }
        }

        // Check JobworkerpServer indices
        for (id, server) in &self.jobworkerp_servers {
            if let Some(data) = &server.data {
                let reverse_id = self.jobworkerp_server_name_index.get(&data.name);
                if reverse_id != Some(id) {
                    return Err(anyhow::anyhow!(
                        "Index inconsistency: jobworkerp_server id={}, name={} (reverse_id={:?})",
                        id.value,
                        data.name,
                        reverse_id
                    ));
                }
            }
        }

        for (name, id) in &self.jobworkerp_server_name_index {
            if !self.jobworkerp_servers.contains_key(id) {
                return Err(anyhow::anyhow!(
                    "Orphaned index entry: jobworkerp_server name={}, id={} (no server found)",
                    name,
                    id.value
                ));
            }
        }

        // Check SlackEventHandler indices
        for (id, slack_handler) in &self.slack_event_handlers {
            if let Some(data) = &slack_handler.data {
                let reverse_id = self.slack_event_handler_name_index.get(&data.name);
                if reverse_id != Some(id) {
                    return Err(anyhow::anyhow!(
                        "Index inconsistency: slack_event_handler id={}, name={} (reverse_id={:?})",
                        id.value,
                        data.name,
                        reverse_id
                    ));
                }
            }
        }

        for (name, id) in &self.slack_event_handler_name_index {
            if !self.slack_event_handlers.contains_key(id) {
                return Err(anyhow::anyhow!(
                    "Orphaned index entry: slack_event_handler name={}, id={} (no handler found)",
                    name,
                    id.value
                ));
            }
        }

        tracing::debug!("Index consistency validated successfully");
        Ok(())
    }
}

impl Default for LocalConfigStore {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedLocalConfigStore = Arc<RwLock<LocalConfigStore>>;

#[derive(Debug, Clone)]
pub struct LocalConfigStats {
    pub cron_scheduler_count: usize,
    pub worker_result_handler_count: usize,
    pub jobworkerp_server_count: usize,
    pub slack_event_handler_count: usize,
    pub last_sync: SystemTime,
    pub version: u64,
    pub snapshot_count: usize,
}

impl Default for LocalConfigStats {
    fn default() -> Self {
        Self {
            cron_scheduler_count: 0,
            worker_result_handler_count: 0,
            jobworkerp_server_count: 0,
            slack_event_handler_count: 0,
            last_sync: SystemTime::UNIX_EPOCH,
            version: 0,
            snapshot_count: 0,
        }
    }
}

impl LocalConfigStats {
    pub fn total_count(&self) -> usize {
        self.cron_scheduler_count
            + self.worker_result_handler_count
            + self.jobworkerp_server_count
            + self.slack_event_handler_count
    }
}
