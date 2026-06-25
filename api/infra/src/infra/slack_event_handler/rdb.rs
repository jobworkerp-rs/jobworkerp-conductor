use super::rows::{reaction_operation_to_db, SlackEventHandlerRow};
use crate::error::UiEventHandlerError;
use crate::infra::IdGeneratorWrapper;
use crate::infra::UseIdGenerator;
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use proto::jobworkerp_conductor::data::{
    slack_event_handler_data::ExecutionTarget, SlackEventHandler, SlackEventHandlerData,
    SlackEventHandlerId,
};
use sqlx::Executor;

/// Extract flat DB column values from oneof execution_target.
/// Returns (workflow_url, channel, worker_name, using).
/// Note: channel is String (not Option) because SlackEventHandlerData.channel is a required proto field.
/// In worker mode, empty strings are stored in DB (vs NULL for WorkerResultHandler/CronScheduler
/// where channel is optional).
fn flatten_execution_target(
    data: &SlackEventHandlerData,
) -> (String, String, Option<String>, Option<String>) {
    match &data.execution_target {
        Some(ExecutionTarget::Workflow(wf)) => (
            wf.workflow_url.clone(),
            wf.channel.clone().unwrap_or_default(),
            None,
            None,
        ),
        Some(ExecutionTarget::Worker(w)) => (
            "".to_string(),
            "".to_string(),
            Some(w.worker_name.clone()),
            w.using.as_ref().filter(|s| !s.is_empty()).cloned(),
        ),
        None => {
            // Fallback to deprecated fields 11/12
            (data.workflow_url.clone(), data.channel.clone(), None, None)
        }
    }
}

#[async_trait]
pub trait SlackEventHandlerRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    async fn create<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        slack_event_handler: &SlackEventHandlerData,
    ) -> Result<SlackEventHandlerId> {
        let id: i64 = self.id_generator().generate_id()?;
        let (db_workflow_url, db_channel, db_worker_name, db_using) =
            flatten_execution_target(slack_event_handler);
        let res = sqlx::query::<Rdb>(
            "INSERT INTO `slack_event_handler` (
            `id`,
            `name`,
            `description`,
            `enabled`,
            `slack_channel_id`,
            `message_pattern`,
            `mention_required`,
            `reaction_names`,
            `reaction_operation`,
            `reaction_user_filter`,
            `jobworkerp_server_id`,
            `workflow_url`,
            `channel`,
            `timeout_sec`,
            `args`,
            `worker_name`,
            `using`,
            `created_at`,
            `updated_at`
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(id)
        .bind(&slack_event_handler.name)
        .bind(&slack_event_handler.description)
        .bind(slack_event_handler.enabled)
        .bind(&slack_event_handler.slack_channel_id)
        .bind(&slack_event_handler.message_pattern)
        .bind(slack_event_handler.mention_required)
        .bind(&slack_event_handler.reaction_names)
        .bind(reaction_operation_to_db(
            slack_event_handler.reaction_operation,
        ))
        .bind(&slack_event_handler.reaction_user_filter)
        .bind(
            slack_event_handler
                .jobworkerp_server_id
                .as_ref()
                .map(|id| id.value),
        )
        .bind(&db_workflow_url)
        .bind(&db_channel)
        .bind(slack_event_handler.timeout_sec)
        .bind(&slack_event_handler.args)
        .bind(&db_worker_name)
        .bind(&db_using)
        .bind(slack_event_handler.created_at)
        .bind(slack_event_handler.updated_at)
        .execute(tx)
        .await;

        match res {
            Ok(r) if r.rows_affected() > 0 => Ok(SlackEventHandlerId { value: id }),
            Ok(_) => Err(UiEventHandlerError::RuntimeError(format!(
                "Cannot insert slack_event_handler (logic error?): {slack_event_handler:?}"
            ))
            .into()),
            Err(e) => {
                // Check for unique constraint violation
                if let sqlx::Error::Database(db_error) = &e {
                    if db_error
                        .code()
                        .is_some_and(|code| code == "2067" || code == "1062")
                    {
                        return Err(UiEventHandlerError::AlreadyExists(format!(
                            "SlackEventHandler name '{}' already exists",
                            &slack_event_handler.name
                        ))
                        .into());
                    }
                }
                Err(UiEventHandlerError::DBError(e).into())
            }
        }
    }

    async fn update<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &SlackEventHandlerId,
        slack_event_handler: &SlackEventHandlerData,
    ) -> Result<bool> {
        let (db_workflow_url, db_channel, db_worker_name, db_using) =
            flatten_execution_target(slack_event_handler);
        sqlx::query(
            "UPDATE `slack_event_handler` SET
            `name` = ?,
            `description` = ?,
            `enabled` = ?,
            `slack_channel_id` = ?,
            `message_pattern` = ?,
            `mention_required` = ?,
            `reaction_names` = ?,
            `reaction_operation` = ?,
            `reaction_user_filter` = ?,
            `jobworkerp_server_id` = ?,
            `workflow_url` = ?,
            `channel` = ?,
            `timeout_sec` = ?,
            `args` = ?,
            `worker_name` = ?,
            `using` = ?,
            `updated_at` = ?
            WHERE id = ?",
        )
        .bind(&slack_event_handler.name)
        .bind(&slack_event_handler.description)
        .bind(slack_event_handler.enabled)
        .bind(&slack_event_handler.slack_channel_id)
        .bind(&slack_event_handler.message_pattern)
        .bind(slack_event_handler.mention_required)
        .bind(&slack_event_handler.reaction_names)
        .bind(reaction_operation_to_db(
            slack_event_handler.reaction_operation,
        ))
        .bind(&slack_event_handler.reaction_user_filter)
        .bind(
            slack_event_handler
                .jobworkerp_server_id
                .as_ref()
                .map(|id| id.value),
        )
        .bind(&db_workflow_url)
        .bind(&db_channel)
        .bind(slack_event_handler.timeout_sec)
        .bind(&slack_event_handler.args)
        .bind(&db_worker_name)
        .bind(&db_using)
        .bind(slack_event_handler.updated_at)
        .bind(id.value)
        .execute(tx)
        .await
        .map(|r| r.rows_affected() > 0)
        .context("Failed to update slack_event_handler")
    }

    async fn delete<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &SlackEventHandlerId,
    ) -> Result<bool> {
        sqlx::query("DELETE FROM `slack_event_handler` WHERE id = ?")
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .context("Failed to delete slack_event_handler")
    }

    async fn find(&self, id: &SlackEventHandlerId) -> Result<Option<SlackEventHandler>> {
        let row: Option<SlackEventHandlerRow> =
            sqlx::query_as("SELECT * FROM `slack_event_handler` WHERE id = ?")
                .bind(id.value)
                .fetch_optional(self.db_pool())
                .await
                .context("Failed to find slack_event_handler by id")?;
        Ok(row.map(|r| r.to_proto()))
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<SlackEventHandler>> {
        let row: Option<SlackEventHandlerRow> =
            sqlx::query_as("SELECT * FROM `slack_event_handler` WHERE name = ?")
                .bind(name)
                .fetch_optional(self.db_pool())
                .await
                .context("Failed to find slack_event_handler by name")?;
        Ok(row.map(|r| r.to_proto()))
    }

    async fn find_all(&self) -> Result<Vec<SlackEventHandler>> {
        let rows: Vec<SlackEventHandlerRow> = sqlx::query_as("SELECT * FROM `slack_event_handler`")
            .fetch_all(self.db_pool())
            .await
            .context("Failed to find all slack_event_handlers")?;
        Ok(rows.into_iter().map(|r| r.to_proto()).collect())
    }

    async fn find_all_enabled(&self) -> Result<Vec<SlackEventHandler>> {
        let rows: Vec<SlackEventHandlerRow> =
            sqlx::query_as("SELECT * FROM `slack_event_handler` WHERE enabled = true")
                .fetch_all(self.db_pool())
                .await
                .context("Failed to find enabled slack_event_handlers")?;
        Ok(rows.into_iter().map(|r| r.to_proto()).collect())
    }

    async fn count(&self) -> Result<i64> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM `slack_event_handler`")
            .fetch_one(self.db_pool())
            .await
            .context("Failed to count slack_event_handlers")?;
        Ok(count.0)
    }
}

pub struct SlackEventHandlerRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    rdb_pool: RdbPool,
}

impl SlackEventHandlerRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, rdb_pool: RdbPool) -> Self {
        Self {
            id_generator,
            rdb_pool,
        }
    }
}

impl UseIdGenerator for SlackEventHandlerRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl UseRdbPool for SlackEventHandlerRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        &self.rdb_pool
    }
}

impl SlackEventHandlerRepository for SlackEventHandlerRepositoryImpl {}

pub trait UseSlackEventHandlerRepository {
    fn slack_event_handler_repository(&self) -> &SlackEventHandlerRepositoryImpl;
}
