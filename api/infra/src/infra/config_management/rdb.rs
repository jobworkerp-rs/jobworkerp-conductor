use crate::error::UiEventHandlerError;
use crate::infra::cron_scheduler::rows::CronSchedulerRow;
use crate::infra::jobworkerp_server::rows::JobworkerpServerRow;
use crate::infra::slack_event_handler::rows::SlackEventHandlerRow;
use crate::infra::worker_result_handler::rows::WorkerResultHandlerRow;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, RdbTransaction, UseRdbPool};
use proto::jobworkerp_conductor::data::{
    CronSchedulerData, JobworkerpServerData, SlackEventHandlerData, WorkerResultHandlerData,
};
use sqlx::Row;

/// The tables a bulk delete clears, dependents first so the servers they
/// reference go last.
const TABLES_DEPENDENTS_FIRST: [&str; 4] = [
    "worker_result_handler",
    "cron_scheduler",
    "slack_event_handler",
    "jobworkerp_server",
];

/// What a bulk write runs on: a transaction's connection, so it can be rolled back.
pub type Connection = <Rdb as sqlx::Database>::Connection;

/// Every stored setting's id and name: what a bulk delete must announce.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ConfigIds {
    pub servers: Vec<(i64, String)>,
    pub schedulers: Vec<(i64, String)>,
    pub listeners: Vec<(i64, String)>,
    pub slack_handlers: Vec<(i64, String)>,
}

/// Every stored setting, each kind in name order so the same settings always
/// export to the same file. Servers carry their ids, which the others name.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StoredConfigs {
    pub servers: Vec<(i64, JobworkerpServerData)>,
    pub schedulers: Vec<CronSchedulerData>,
    pub listeners: Vec<WorkerResultHandlerData>,
    pub slack_handlers: Vec<SlackEventHandlerData>,
}

#[async_trait]
pub trait ConfigManagementRepository: UseRdbPool + Sync + Send {
    /// The transaction a bulk write runs in. SQLite starts it IMMEDIATE: a
    /// deferred one that reads first fails at once ("database is locked")
    /// when it goes on to write while another connection is writing, instead
    /// of waiting for it.
    async fn begin_bulk_write(&self) -> Result<RdbTransaction<'static>> {
        #[cfg(not(feature = "mysql"))]
        let tx = self.db_pool().begin_with("BEGIN IMMEDIATE").await;
        #[cfg(feature = "mysql")]
        let tx = self.db_pool().begin().await;
        Ok(tx.map_err(UiEventHandlerError::DBError)?)
    }

    async fn list_config_ids(&self, conn: &mut Connection) -> Result<ConfigIds> {
        async fn ids(conn: &mut Connection, table: &str) -> Result<Vec<(i64, String)>> {
            let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT `id`, `name` FROM `{table}`"
            )))
            .fetch_all(&mut *conn)
            .await
            .map_err(UiEventHandlerError::DBError)?;
            Ok(rows
                .into_iter()
                .map(|row| (row.get("id"), row.get("name")))
                .collect())
        }
        Ok(ConfigIds {
            servers: ids(conn, "jobworkerp_server").await?,
            schedulers: ids(conn, "cron_scheduler").await?,
            listeners: ids(conn, "worker_result_handler").await?,
            slack_handlers: ids(conn, "slack_event_handler").await?,
        })
    }

    /// Deletes every setting; execution history (`execution_ref`) stays.
    /// DELETE rather than TRUNCATE: MySQL commits a TRUNCATE on the spot,
    /// which would defeat a later rollback.
    async fn delete_all_configs(&self, conn: &mut Connection) -> Result<()> {
        for table in TABLES_DEPENDENTS_FIRST {
            sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM `{table}`")))
                .execute(&mut *conn)
                .await
                .map_err(UiEventHandlerError::DBError)?;
        }
        Ok(())
    }

    async fn load_all_configs(&self) -> Result<StoredConfigs> {
        let pool = self.db_pool();
        let servers: Vec<JobworkerpServerRow> = fetch_all(pool, "jobworkerp_server").await?;
        let schedulers: Vec<CronSchedulerRow> = fetch_all(pool, "cron_scheduler").await?;
        let listeners: Vec<WorkerResultHandlerRow> =
            fetch_all(pool, "worker_result_handler").await?;
        let slack_handlers: Vec<SlackEventHandlerRow> =
            fetch_all(pool, "slack_event_handler").await?;
        Ok(StoredConfigs {
            servers: servers
                .iter()
                .map(|row| (row.id, row.to_proto().data.unwrap_or_default()))
                .collect(),
            schedulers: schedulers
                .iter()
                .map(|row| Ok(row.to_proto()?.data.unwrap_or_default()))
                .collect::<Result<_>>()?,
            listeners: listeners
                .iter()
                .map(|row| row.to_proto().data.unwrap_or_default())
                .collect(),
            slack_handlers: slack_handlers
                .iter()
                .map(|row| row.to_proto().data.unwrap_or_default())
                .collect(),
        })
    }
}

async fn fetch_all<T>(pool: &RdbPool, table: &str) -> Result<Vec<T>>
where
    T: for<'r> sqlx::FromRow<'r, <Rdb as sqlx::Database>::Row> + Send + Unpin,
{
    Ok(sqlx::query_as::<Rdb, T>(sqlx::AssertSqlSafe(format!(
        "SELECT * FROM `{table}` ORDER BY `name`"
    )))
    .fetch_all(pool)
    .await
    .map_err(UiEventHandlerError::DBError)?)
}

pub struct ConfigManagementRepositoryImpl {
    pool: &'static RdbPool,
}

impl ConfigManagementRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ConfigManagementRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

#[async_trait]
impl ConfigManagementRepository for ConfigManagementRepositoryImpl {}
