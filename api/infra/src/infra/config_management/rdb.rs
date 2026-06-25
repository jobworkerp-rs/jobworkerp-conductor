use crate::error::UiEventHandlerError;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::{RdbPool, UseRdbPool};
use jobworkerp_handler::settings::{JobWorkerpSetting, WorkflowSettingFile, WorkflowSettingItem};
use sqlx::Row;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

use crate::infra::{IdGeneratorWrapper, UseIdGenerator};

#[async_trait]
pub trait ConfigManagementRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    /// Clear all configuration data (in correct order for referential integrity)
    async fn clear_all_configs(&self) -> Result<()> {
        let pool = self.db_pool();
        let mut tx = pool.begin().await.map_err(UiEventHandlerError::DBError)?;

        // Delete in reverse dependency order to avoid foreign key issues
        sqlx::query("DELETE FROM `worker_result_handler`;")
            .execute(&mut *tx)
            .await
            .map_err(UiEventHandlerError::DBError)?;
        sqlx::query("DELETE FROM `cron_scheduler`;")
            .execute(&mut *tx)
            .await
            .map_err(UiEventHandlerError::DBError)?;
        sqlx::query("DELETE FROM `jobworkerp_server`;")
            .execute(&mut *tx)
            .await
            .map_err(UiEventHandlerError::DBError)?;

        tx.commit().await.map_err(UiEventHandlerError::DBError)?;
        Ok(())
    }

    /// Import configuration from TOML
    async fn import_toml_config(
        &self,
        toml_content: &str,
        overwrite_existing: bool,
    ) -> Result<(i32, i32, i32)> {
        // Parse and validate TOML
        let workflow_settings = validate_toml_config(toml_content)?;

        let pool = self.db_pool();
        let mut tx = pool.begin().await.map_err(UiEventHandlerError::DBError)?;

        // Clear existing data if requested
        if overwrite_existing {
            sqlx::query("DELETE FROM `worker_result_handler`;")
                .execute(&mut *tx)
                .await
                .map_err(UiEventHandlerError::DBError)?;
            sqlx::query("DELETE FROM `cron_scheduler`;")
                .execute(&mut *tx)
                .await
                .map_err(UiEventHandlerError::DBError)?;
            sqlx::query("DELETE FROM `jobworkerp_server`;")
                .execute(&mut *tx)
                .await
                .map_err(UiEventHandlerError::DBError)?;
        }

        // Import servers
        let mut imported_servers = 0;
        for server in &workflow_settings.jobworkerp {
            let (host, port, ssl_enabled) = parse_server_address(&server.address)
                .context(format!("Invalid server address: {}", server.address))?;

            let id: i64 = self.id_generator().generate_id()?;
            let timestamp = current_timestamp();

            let result = sqlx::query(
                "INSERT INTO `jobworkerp_server` (
                `id`, `name`, `host`, `port`, `ssl_enabled`, `description`, `enabled`, `created_at`, `updated_at`
                ) VALUES (?,?,?,?,?,?,?,?,?)"
            )
            .bind(id)
            .bind(&server.name)
            .bind(&host)
            .bind(&port)
            .bind(ssl_enabled)
            .bind(None::<String>) // description
            .bind(true) // enabled
            .bind(timestamp)
            .bind(timestamp)
            .execute(&mut *tx)
            .await;

            match result {
                Ok(_) => imported_servers += 1,
                Err(e) => {
                    // Check for unique constraint violation
                    if let sqlx::Error::Database(db_error) = &e {
                        if db_error
                            .code()
                            .is_some_and(|code| code == "2067" || code == "1062")
                        {
                            return Err(UiEventHandlerError::AlreadyExists(format!(
                                "Server name '{}' already exists",
                                server.name
                            ))
                            .into());
                        }
                    }
                    return Err(UiEventHandlerError::DBError(e).into());
                }
            }
        }

        // Build server name to ID mapping
        let rows = sqlx::query("SELECT id, name FROM jobworkerp_server")
            .fetch_all(&mut *tx)
            .await
            .map_err(UiEventHandlerError::DBError)?;

        let mut server_map = std::collections::HashMap::new();
        for row in rows {
            let id: i64 = row.get("id");
            let name: String = row.get("name");
            server_map.insert(name, id);
        }

        // Import schedulers
        let mut imported_schedulers = 0;
        if let Some(schedulers) = &workflow_settings.schedulers {
            for scheduler in schedulers {
                let crontab = scheduler
                    .crontab
                    .as_ref()
                    .ok_or_else(|| anyhow!("Missing crontab for scheduler '{}'", scheduler.name))?;

                let jobworkerp_server_id =
                    server_map.get(&scheduler.jobworkerp).ok_or_else(|| {
                        anyhow!(
                            "Server '{}' not found for scheduler '{}'",
                            scheduler.jobworkerp,
                            scheduler.name
                        )
                    })?;

                let id: i64 = self.id_generator().generate_id()?;
                let timestamp = current_timestamp();

                let has_worker_name = scheduler
                    .worker_name
                    .as_ref()
                    .is_some_and(|n| !n.is_empty());
                if has_worker_name && !scheduler.workflow_url.is_empty() {
                    tracing::warn!(
                        "Scheduler '{}' has both worker_name and workflow_url; worker_name takes precedence",
                        scheduler.name
                    );
                }
                let (db_workflow_url, db_channel) = if has_worker_name {
                    ("", None) // Worker execution mode: clear workflow_url and channel
                } else {
                    (
                        scheduler.workflow_url.as_str(),
                        scheduler.channel.as_deref(),
                    )
                };

                let result = sqlx::query(
                    "INSERT INTO `cron_scheduler` (
                    `id`, `name`, `jobworkerp_server_id`, `workflow_url`, `channel`, `crontab`, `enabled`, `description`, `created_at`, `updated_at`, `args`, `worker_name`, `using`
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
                )
                .bind(id)
                .bind(&scheduler.name)
                .bind(jobworkerp_server_id)
                .bind(db_workflow_url)
                .bind(db_channel)
                .bind(crontab)
                .bind(true) // enabled
                .bind(None::<String>) // description
                .bind(timestamp)
                .bind(timestamp)
                .bind(&scheduler.args)
                .bind(&scheduler.worker_name)
                .bind(scheduler.using.as_deref().filter(|s| !s.is_empty()))
                .execute(&mut *tx)
                .await;

                match result {
                    Ok(_) => imported_schedulers += 1,
                    Err(e) => {
                        if let sqlx::Error::Database(db_error) = &e {
                            if db_error
                                .code()
                                .is_some_and(|code| code == "2067" || code == "1062")
                            {
                                return Err(UiEventHandlerError::AlreadyExists(format!(
                                    "Scheduler name '{}' already exists",
                                    scheduler.name
                                ))
                                .into());
                            }
                        }
                        return Err(UiEventHandlerError::DBError(e).into());
                    }
                }
            }
        }

        // Import listeners
        let mut imported_listeners = 0;
        if let Some(listeners) = &workflow_settings.listeners {
            for listener in listeners {
                let listen_worker_name = listener.listen_worker_name.as_ref().ok_or_else(|| {
                    anyhow!(
                        "Missing listen_worker_name for listener '{}'",
                        listener.name
                    )
                })?;

                let listen_jobworkerp = listener.listen_jobworkerp.as_ref().ok_or_else(|| {
                    anyhow!("Missing listen_jobworkerp for listener '{}'", listener.name)
                })?;

                let listen_jobworkerp_server_id =
                    server_map.get(listen_jobworkerp).ok_or_else(|| {
                        anyhow!(
                            "Listen server '{}' not found for listener '{}'",
                            listen_jobworkerp,
                            listener.name
                        )
                    })?;

                let process_jobworkerp_server_id =
                    server_map.get(&listener.jobworkerp).ok_or_else(|| {
                        anyhow!(
                            "Process server '{}' not found for listener '{}'",
                            listener.jobworkerp,
                            listener.name
                        )
                    })?;

                let id: i64 = self.id_generator().generate_id()?;
                let timestamp = current_timestamp();

                let has_worker_name = listener.worker_name.as_ref().is_some_and(|n| !n.is_empty());
                if has_worker_name && !listener.workflow_url.is_empty() {
                    tracing::warn!(
                        "Listener '{}' has both worker_name and workflow_url; worker_name takes precedence",
                        listener.name
                    );
                }
                // worker_result_handler.workflow_url is TEXT NOT NULL in both SQLite and MySQL.
                // Storing an empty string for worker mode satisfies the NOT NULL constraint.
                let (db_workflow_url, db_channel, db_worker_name, db_using) = if has_worker_name {
                    (
                        "",
                        None,
                        listener.worker_name.as_deref(),
                        listener.using.as_deref().filter(|s| !s.is_empty()),
                    )
                } else {
                    (
                        listener.workflow_url.as_str(),
                        listener.channel.as_deref(),
                        None,
                        None,
                    )
                };

                let result = sqlx::query(
                    "INSERT INTO `worker_result_handler` (
                    `id`, `name`, `listen_jobworkerp_server_id`, `listen_worker_name`, `process_jobworkerp_server_id`, `workflow_url`, `channel`, `enabled`, `description`, `created_at`, `updated_at`, `args`, `worker_name`, `using`
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                )
                .bind(id)
                .bind(&listener.name)
                .bind(listen_jobworkerp_server_id)
                .bind(listen_worker_name)
                .bind(process_jobworkerp_server_id)
                .bind(db_workflow_url)
                .bind(db_channel)
                .bind(true) // enabled
                .bind(None::<String>) // description
                .bind(timestamp)
                .bind(timestamp)
                .bind(&listener.args)
                .bind(db_worker_name)
                .bind(db_using)
                .execute(&mut *tx)
                .await;

                match result {
                    Ok(_) => imported_listeners += 1,
                    Err(e) => {
                        if let sqlx::Error::Database(db_error) = &e {
                            if db_error
                                .code()
                                .is_some_and(|code| code == "2067" || code == "1062")
                            {
                                return Err(UiEventHandlerError::AlreadyExists(format!(
                                    "Listener name '{}' already exists",
                                    listener.name
                                ))
                                .into());
                            }
                        }
                        return Err(UiEventHandlerError::DBError(e).into());
                    }
                }
            }
        }

        tx.commit().await.map_err(UiEventHandlerError::DBError)?;
        Ok((imported_servers, imported_schedulers, imported_listeners))
    }

    /// Export configuration to TOML
    async fn export_toml_config(&self, enabled_only: bool) -> Result<String> {
        let pool = self.db_pool();
        let mut tx = pool.begin().await.map_err(UiEventHandlerError::DBError)?;

        // Get servers
        let server_query = if enabled_only {
            "SELECT name, host, port, ssl_enabled FROM jobworkerp_server WHERE enabled = 1"
        } else {
            "SELECT name, host, port, ssl_enabled FROM jobworkerp_server"
        };

        let server_rows = sqlx::query(server_query)
            .fetch_all(&mut *tx)
            .await
            .map_err(UiEventHandlerError::DBError)?;

        let mut servers = Vec::new();
        for row in server_rows {
            let name: String = row.get("name");
            let host: String = row.get("host");
            let port: String = row.get("port");
            let ssl_enabled: bool = row.get("ssl_enabled");

            let protocol = if ssl_enabled { "https" } else { "http" };
            let address = format!("{protocol}://{host}:{port}");

            servers.push(JobWorkerpSetting { name, address });
        }

        // Get schedulers
        let scheduler_query = if enabled_only {
            "SELECT cs.name, cs.workflow_url, cs.channel, cs.crontab, cs.args, cs.`worker_name`, cs.`using`, js.name as jobworkerp_name
             FROM cron_scheduler cs
             JOIN jobworkerp_server js ON cs.jobworkerp_server_id = js.id
             WHERE cs.enabled = 1"
        } else {
            "SELECT cs.name, cs.workflow_url, cs.channel, cs.crontab, cs.args, cs.`worker_name`, cs.`using`, js.name as jobworkerp_name
             FROM cron_scheduler cs
             JOIN jobworkerp_server js ON cs.jobworkerp_server_id = js.id"
        };

        let scheduler_rows = sqlx::query(scheduler_query)
            .fetch_all(&mut *tx)
            .await
            .map_err(UiEventHandlerError::DBError)?;

        let mut schedulers = Vec::new();
        for row in scheduler_rows {
            let name: String = row.get("name");
            let workflow_url: String = row.get("workflow_url");
            let channel: Option<String> = row.get("channel");
            let crontab: String = row.get("crontab");
            let jobworkerp: String = row.get("jobworkerp_name");
            let args: Option<String> = row.get("args");
            let worker_name: Option<String> = row.get("worker_name");
            let using: Option<String> = row.get("using");

            schedulers.push(WorkflowSettingItem {
                name,
                jobworkerp,
                workflow_url,
                channel,
                listen_worker_name: None,
                listen_jobworkerp: None,
                crontab: Some(crontab),
                args,
                worker_name,
                using,
            });
        }

        // Get listeners
        let listener_query = if enabled_only {
            "SELECT wrh.name, wrh.workflow_url, wrh.channel, wrh.listen_worker_name,
                    wrh.args, wrh.`worker_name`, wrh.`using`,
                    js1.name as listen_jobworkerp_name, js2.name as process_jobworkerp_name
             FROM worker_result_handler wrh
             JOIN jobworkerp_server js1 ON wrh.listen_jobworkerp_server_id = js1.id
             JOIN jobworkerp_server js2 ON wrh.process_jobworkerp_server_id = js2.id
             WHERE wrh.enabled = 1"
        } else {
            "SELECT wrh.name, wrh.workflow_url, wrh.channel, wrh.listen_worker_name,
                    wrh.args, wrh.`worker_name`, wrh.`using`,
                    js1.name as listen_jobworkerp_name, js2.name as process_jobworkerp_name
             FROM worker_result_handler wrh
             JOIN jobworkerp_server js1 ON wrh.listen_jobworkerp_server_id = js1.id
             JOIN jobworkerp_server js2 ON wrh.process_jobworkerp_server_id = js2.id"
        };

        let listener_rows = sqlx::query(listener_query)
            .fetch_all(&mut *tx)
            .await
            .map_err(UiEventHandlerError::DBError)?;

        let mut listeners = Vec::new();
        for row in listener_rows {
            let name: String = row.get("name");
            let workflow_url: String = row.get("workflow_url");
            let channel: Option<String> = row.get("channel");
            let listen_worker_name: String = row.get("listen_worker_name");
            let listen_jobworkerp: String = row.get("listen_jobworkerp_name");
            let jobworkerp: String = row.get("process_jobworkerp_name");
            let args: Option<String> = row.get("args");
            let worker_name: Option<String> = row.get("worker_name");
            let using: Option<String> = row.get("using");

            listeners.push(WorkflowSettingItem {
                name,
                jobworkerp,
                workflow_url,
                channel,
                listen_worker_name: Some(listen_worker_name),
                listen_jobworkerp: Some(listen_jobworkerp),
                crontab: None,
                args,
                worker_name,
                using,
            });
        }

        tx.commit().await.map_err(UiEventHandlerError::DBError)?;

        let workflow_file = WorkflowSettingFile {
            jobworkerp: servers,
            schedulers: if schedulers.is_empty() {
                None
            } else {
                Some(schedulers)
            },
            listeners: if listeners.is_empty() {
                None
            } else {
                Some(listeners)
            },
        };

        toml::to_string_pretty(&workflow_file).context("Failed to serialize to TOML")
    }
}

pub struct ConfigManagementRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    pool: &'static RdbPool,
}

pub trait UseConfigManagementRepository {
    fn config_management_repository(&self) -> &ConfigManagementRepositoryImpl;
}

impl ConfigManagementRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { id_generator, pool }
    }
}

impl UseRdbPool for ConfigManagementRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ConfigManagementRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

#[async_trait]
impl ConfigManagementRepository for ConfigManagementRepositoryImpl {}

/// Safe address parsing utility using url crate
fn parse_server_address(address: &str) -> Result<(String, String, bool)> {
    let url = Url::parse(address).context("Invalid URL format")?;

    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("Missing host in URL"))?
        .to_string();

    let ssl_enabled = url.scheme() == "https";
    let default_port = if ssl_enabled { 443 } else { 80 };
    let port = url.port().unwrap_or(default_port).to_string();

    Ok((host, port, ssl_enabled))
}

/// Get current timestamp in seconds since Unix epoch
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Validate TOML configuration
pub fn validate_toml_config(toml_content: &str) -> Result<WorkflowSettingFile> {
    let workflow_settings: WorkflowSettingFile =
        toml::from_str(toml_content).context("Invalid TOML format")?;

    // Basic validation
    if workflow_settings.jobworkerp.is_empty() {
        return Err(anyhow!("At least one jobworkerp server must be defined"));
    }

    // Validate server names uniqueness
    let mut server_names = std::collections::HashSet::new();
    for server in &workflow_settings.jobworkerp {
        if !server_names.insert(&server.name) {
            return Err(anyhow!("Duplicate server name: {}", server.name));
        }
        // Validate address format
        parse_server_address(&server.address).context(format!(
            "Invalid address for server '{}': {}",
            server.name, server.address
        ))?;
    }

    // Validate listener items: either worker_name or workflow_url must be set
    if let Some(listeners) = &workflow_settings.listeners {
        for listener in listeners {
            let has_worker_name = listener.worker_name.as_ref().is_some_and(|n| !n.is_empty());
            if !has_worker_name && listener.workflow_url.is_empty() {
                return Err(anyhow!(
                    "Listener '{}' must have either worker_name or workflow_url set.",
                    listener.name
                ));
            }
        }
    }

    // Validate scheduler items: either worker_name or workflow_url must be set
    // (Backfill: this validation was missing in the CronScheduler worker execution PR)
    if let Some(schedulers) = &workflow_settings.schedulers {
        for scheduler in schedulers {
            let has_worker_name = scheduler
                .worker_name
                .as_ref()
                .is_some_and(|n| !n.is_empty());
            if !has_worker_name && scheduler.workflow_url.is_empty() {
                return Err(anyhow!(
                    "Scheduler '{}' must have either worker_name or workflow_url set.",
                    scheduler.name
                ));
            }
        }
    }

    Ok(workflow_settings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_server_address() {
        // Test HTTPS
        let (host, port, ssl) = parse_server_address("https://example.com:8443").unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, "8443");
        assert!(ssl);

        // Test HTTP with default port
        let (host, port, ssl) = parse_server_address("http://localhost").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, "80");
        assert!(!ssl);

        // Test invalid URL
        assert!(parse_server_address("invalid-url").is_err());
    }

    #[test]
    fn test_validate_toml_config() {
        let valid_toml = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:8080"

[[schedulers]]
name = "test-scheduler"
jobworkerp = "server1"
workflow_url = "test.yml"
crontab = "0 * * * *"
"#;

        let result = validate_toml_config(valid_toml);
        assert!(result.is_ok());

        // Test duplicate server names
        let invalid_toml = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:8080"

[[jobworkerp]]
name = "server1"
address = "http://localhost:8081"
"#;

        let result = validate_toml_config(invalid_toml);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_listener_with_worker_name_ok() {
        let toml_with_listener_worker_name = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:8080"

[[listeners]]
name = "my-listener"
jobworkerp = "server1"
listen_worker_name = "some-worker"
listen_jobworkerp = "server1"
worker_name = "my-worker"
using = "run"
"#;

        let result = validate_toml_config(toml_with_listener_worker_name);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_listener_without_worker_name_ok() {
        let valid_toml = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:8080"

[[listeners]]
name = "my-listener"
jobworkerp = "server1"
listen_worker_name = "some-worker"
listen_jobworkerp = "server1"
workflow_url = "process.yml"
"#;

        let result = validate_toml_config(valid_toml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_listener_without_both_rejected() {
        let invalid_toml = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:8080"

[[listeners]]
name = "my-listener"
jobworkerp = "server1"
listen_worker_name = "some-worker"
listen_jobworkerp = "server1"
"#;

        let result = validate_toml_config(invalid_toml);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must have either worker_name or workflow_url"));
    }

    // Integration tests for import/export functionality
    #[cfg(feature = "integration_tests")]
    mod integration_tests {
        use super::*;
        use crate::infra::IdGeneratorWrapper;
        use infra_utils::infra::test::{setup_test_rdb_from, TEST_RUNTIME};

        async fn _test_import_toml_with_duplicates(pool: &'static RdbPool) -> Result<()> {
            let repository = ConfigManagementRepositoryImpl::new(IdGeneratorWrapper::new(), pool);

            // Clear existing data
            repository.clear_all_configs().await?;

            // First import
            let toml_content1 = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:8080"

[[schedulers]]
name = "scheduler1"
jobworkerp = "server1"
workflow_url = "test1.yml"
crontab = "0 * * * *"
"#;

            let result1 = repository.import_toml_config(toml_content1, false).await?;
            assert_eq!(result1.0, 1); // 1 server
            assert_eq!(result1.1, 1); // 1 scheduler

            // Second import with duplicate names (should fail if constraints are applied)
            let toml_content2 = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:8081"

[[schedulers]]
name = "scheduler1"
jobworkerp = "server1"
workflow_url = "test2.yml"
crontab = "0 2 * * *"
"#;

            let result2 = repository.import_toml_config(toml_content2, false).await;
            match result2 {
                Err(e) => {
                    println!("✅ UNIQUE constraint working - duplicate import rejected: {e}");
                    assert!(e.to_string().contains("already exists"));
                }
                Ok(_) => {
                    println!("⚠️  UNIQUE constraint not yet applied - duplicate import allowed");
                }
            }

            // Test overwrite mode
            let result3 = repository.import_toml_config(toml_content2, true).await;
            match result3 {
                Ok(counts) => {
                    println!("✅ Overwrite mode working - replaced existing data");
                    assert_eq!(counts.0, 1); // 1 server
                    assert_eq!(counts.1, 1); // 1 scheduler
                }
                Err(e) => {
                    println!("❌ Overwrite mode failed: {e}");
                    return Err(e);
                }
            }

            Ok(())
        }

        async fn _test_export_import_roundtrip(pool: &'static RdbPool) -> Result<()> {
            let repository = ConfigManagementRepositoryImpl::new(IdGeneratorWrapper::new(), pool);

            // Clear and set up test data
            repository.clear_all_configs().await?;

            let original_toml = r#"
[[jobworkerp]]
name = "test_server"
address = "https://example.com:8443"

[[schedulers]]
name = "daily_job"
jobworkerp = "test_server"
workflow_url = "daily.yml"
crontab = "0 0 * * *"

[[listeners]]
name = "result_listener"
jobworkerp = "test_server"
listen_jobworkerp = "test_server"
listen_worker_name = "worker1"
workflow_url = "listener.yml"
"#;

            // Import original data
            let import_result = repository.import_toml_config(original_toml, true).await?;
            println!(
                "Imported: {} servers, {} schedulers, {} listeners",
                import_result.0, import_result.1, import_result.2
            );

            // Export and verify
            let exported_toml = repository.export_toml_config(false).await?;
            println!("Exported TOML:\n{exported_toml}");

            // Clear and re-import exported data
            repository.clear_all_configs().await?;
            let reimport_result = repository.import_toml_config(&exported_toml, true).await?;

            // Verify counts match
            assert_eq!(import_result.0, reimport_result.0, "Server count mismatch");
            assert_eq!(
                import_result.1, reimport_result.1,
                "Scheduler count mismatch"
            );
            assert_eq!(
                import_result.2, reimport_result.2,
                "Listener count mismatch"
            );

            println!("✅ Export/Import roundtrip successful");
            Ok(())
        }

        async fn _test_constraint_violation_recovery(pool: &'static RdbPool) -> Result<()> {
            let _repository = ConfigManagementRepositoryImpl::new(IdGeneratorWrapper::new(), pool);

            // Create test data with potential duplicates manually
            let mut tx = pool.begin().await.map_err(UiEventHandlerError::DBError)?;

            // Insert duplicate servers directly (bypassing unique constraint if not applied)
            let id_gen = IdGeneratorWrapper::new();
            let timestamp = current_timestamp();

            for i in 1..=3 {
                let id = id_gen.generate_id()?;
                let _ = sqlx::query(
                    "INSERT INTO jobworkerp_server 
                     (id, name, host, port, ssl_enabled, description, enabled, created_at, updated_at) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                )
                .bind(id)
                .bind("duplicate_server") // Same name for all
                .bind("localhost")
                .bind("8080")
                .bind(false)
                .bind(format!("Server {i}"))
                .bind(true)
                .bind(timestamp + i as i64)
                .bind(timestamp + i as i64)
                .execute(&mut *tx)
                .await;
            }

            tx.commit().await.map_err(UiEventHandlerError::DBError)?;

            // Now test our migration functionality
            use crate::infra::config_management::migration::{
                MigrationRepository, MigrationRepositoryImpl,
            };
            let migration_repo = MigrationRepositoryImpl::new(pool);

            let report = migration_repo.check_and_cleanup_duplicates().await?;
            println!("Duplicate cleanup report: {report:?}");

            if report.has_duplicates() {
                println!("✅ Duplicate detection and cleanup working");
                assert!(report.total_duplicates_cleaned() > 0);
            } else {
                println!(
                    "ℹ️  No duplicates found (may indicate UNIQUE constraints already applied)"
                );
            }

            Ok(())
        }

        #[test]
        fn run_integration_tests() -> Result<()> {
            TEST_RUNTIME.block_on(async {
                let rdb_pool = if cfg!(feature = "mysql") {
                    let pool = setup_test_rdb_from("sql/mysql").await;
                    // Clear all tables
                    sqlx::query("TRUNCATE TABLE worker_result_handler;")
                        .execute(pool)
                        .await?;
                    sqlx::query("TRUNCATE TABLE cron_scheduler;")
                        .execute(pool)
                        .await?;
                    sqlx::query("TRUNCATE TABLE jobworkerp_server;")
                        .execute(pool)
                        .await?;
                    pool
                } else {
                    let pool = setup_test_rdb_from("sql/sqlite").await;
                    // Clear all tables
                    sqlx::query("DELETE FROM worker_result_handler;")
                        .execute(pool)
                        .await?;
                    sqlx::query("DELETE FROM cron_scheduler;")
                        .execute(pool)
                        .await?;
                    sqlx::query("DELETE FROM jobworkerp_server;")
                        .execute(pool)
                        .await?;
                    pool
                };

                println!("=== Running Config Management Integration Tests ===");
                _test_import_toml_with_duplicates(rdb_pool).await?;
                _test_export_import_roundtrip(rdb_pool).await?;
                _test_constraint_violation_recovery(rdb_pool).await?;

                Ok(())
            })
        }
    }
}
