use super::rows::CronSchedulerRow;
use crate::error::UiEventHandlerError;
use crate::infra::IdGeneratorWrapper;
use crate::infra::UseIdGenerator;
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use itertools::Itertools;
use proto::jobworkerp_conductor::data::cron_scheduler_data::ExecutionTarget;
use proto::jobworkerp_conductor::data::{CronScheduler, CronSchedulerData, CronSchedulerId};
use sqlx::Executor;

/// Extract flat DB column values from oneof execution_target.
/// Returns (workflow_url, channel, worker_name, using).
/// Note: channel is Option<String> because CronSchedulerData.channel is an optional proto field.
fn flatten_execution_target(
    data: &CronSchedulerData,
) -> (String, Option<String>, Option<String>, Option<String>) {
    match &data.execution_target {
        Some(ExecutionTarget::Workflow(wf)) => {
            (wf.workflow_url.clone(), wf.channel.clone(), None, None)
        }
        Some(ExecutionTarget::Worker(w)) => (
            "".to_string(),
            None,
            Some(w.worker_name.clone()),
            w.using.as_ref().filter(|s| !s.is_empty()).cloned(),
        ),
        None => {
            // Fallback to deprecated fields 3/4
            (data.workflow_url.clone(), data.channel.clone(), None, None)
        }
    }
}

#[async_trait]
pub trait CronSchedulerRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    async fn create<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        cron_scheduler: &CronSchedulerData,
    ) -> Result<CronSchedulerId> {
        let id: i64 = self.id_generator().generate_id()?;
        let (db_workflow_url, db_channel, db_worker_name, db_using) =
            flatten_execution_target(cron_scheduler);
        let res = sqlx::query::<Rdb>(
            "INSERT INTO `cron_scheduler` (
            `id`,
            `name`,
            `jobworkerp_server_id`,
            `workflow_url`,
            `channel`,
            `crontab`,
            `enabled`,
            `description`,
            `created_at`,
            `updated_at`,
            `args`,
            `worker_name`,
            `using`
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(id)
        .bind(&cron_scheduler.name)
        .bind(
            cron_scheduler
                .jobworkerp_server_id
                .as_ref()
                .map(|id| id.value),
        )
        .bind(&db_workflow_url)
        .bind(&db_channel)
        .bind(&cron_scheduler.crontab)
        .bind(cron_scheduler.enabled)
        .bind(&cron_scheduler.description)
        .bind(cron_scheduler.created_at)
        .bind(cron_scheduler.updated_at)
        .bind(&cron_scheduler.args)
        .bind(&db_worker_name)
        .bind(&db_using)
        .execute(tx)
        .await;
        match res {
            Ok(r) if r.rows_affected() > 0 => Ok(CronSchedulerId { value: id }),
            Ok(_) => Err(UiEventHandlerError::RuntimeError(format!(
                "Cannot insert cron_scheduler (logic error?): {cron_scheduler:?}"
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
                            "CronScheduler name '{}' already exists",
                            &cron_scheduler.name
                        ))
                        .into());
                    }
                }
                return Err(UiEventHandlerError::DBError(e).into());
            }
        }
    }

    async fn update<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &CronSchedulerId,
        cron_scheduler: &CronSchedulerData,
    ) -> Result<bool> {
        let (db_workflow_url, db_channel, db_worker_name, db_using) =
            flatten_execution_target(cron_scheduler);
        sqlx::query(
            "UPDATE `cron_scheduler` SET
            `name` = ?,
            `jobworkerp_server_id` = ?,
            `workflow_url` = ?,
            `channel` = ?,
            `crontab` = ?,
            `enabled` = ?,
            `description` = ?,
            `created_at` = ?,
            `updated_at` = ?,
            `args` = ?,
            `worker_name` = ?,
            `using` = ?
            WHERE `id` = ?;",
        )
        .bind(&cron_scheduler.name)
        .bind(
            cron_scheduler
                .jobworkerp_server_id
                .as_ref()
                .map(|id| id.value),
        )
        .bind(&db_workflow_url)
        .bind(&db_channel)
        .bind(&cron_scheduler.crontab)
        .bind(cron_scheduler.enabled)
        .bind(&cron_scheduler.description)
        .bind(cron_scheduler.created_at)
        .bind(cron_scheduler.updated_at)
        .bind(&cron_scheduler.args)
        .bind(&db_worker_name)
        .bind(&db_using)
        .bind(id.value)
        .execute(tx)
        .await
        .map(|r| r.rows_affected() > 0)
        .map_err(UiEventHandlerError::DBError)
        .context(format!("error in update: id = {}", id.value))
    }

    async fn delete(&self, id: &CronSchedulerId) -> Result<bool> {
        self.delete_tx(self.db_pool(), id).await
    }

    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &CronSchedulerId,
    ) -> Result<bool> {
        let del = sqlx::query::<Rdb>("DELETE FROM `cron_scheduler` WHERE `id` = ?;")
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(UiEventHandlerError::DBError)?;
        Ok(del)
    }

    async fn find(&self, id: &CronSchedulerId) -> Result<Option<CronScheduler>> {
        self.find_row_tx(self.db_pool(), id)
            .await
            .map(|r| r.map(|r2| r2.to_proto()))
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<CronScheduler>> {
        self.find_by_name_tx(self.db_pool(), name)
            .await
            .map(|r| r.map(|r2| r2.to_proto()))
    }

    async fn find_row_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &CronSchedulerId,
    ) -> Result<Option<CronSchedulerRow>> {
        sqlx::query_as::<Rdb, CronSchedulerRow>("SELECT * FROM `cron_scheduler` WHERE `id` = ?;")
            .bind(id.value)
            .fetch_optional(tx)
            .await
            .map_err(UiEventHandlerError::DBError)
            .context(format!("error in find: id = {}", id.value))
    }

    async fn find_by_name_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        name: &str,
    ) -> Result<Option<CronSchedulerRow>> {
        sqlx::query_as::<Rdb, CronSchedulerRow>(
            "SELECT * FROM `cron_scheduler` WHERE `name` = ? LIMIT 1;",
        )
        .bind(name)
        .fetch_optional(tx)
        .await
        .map_err(UiEventHandlerError::DBError)
        .context(format!("error in find_by_name: name = {name}"))
    }

    async fn find_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<CronScheduler>> {
        self.find_row_list_tx(self.db_pool(), limit, offset)
            .await
            .map(|r| r.iter().map(|r2| r2.to_proto()).collect_vec())
    }

    async fn find_row_list_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<CronSchedulerRow>> {
        if let Some(l) = limit {
            sqlx::query_as::<_, CronSchedulerRow>(
                "SELECT * FROM `cron_scheduler` ORDER BY `id` DESC LIMIT ? OFFSET ?;",
            )
            .bind(l)
            .bind(offset.unwrap_or(&0i64))
            .fetch_all(tx)
        } else {
            // fetch all!
            sqlx::query_as::<_, CronSchedulerRow>(
                "SELECT * FROM `cron_scheduler` ORDER BY `id` DESC;",
            )
            .fetch_all(tx)
        }
        .await
        .map_err(UiEventHandlerError::DBError)
        .context(format!("error in find_list: ({limit:?}, {offset:?})"))
    }

    async fn count_list_tx<'c, E: Executor<'c, Database = Rdb>>(&self, tx: E) -> Result<i64> {
        sqlx::query_scalar("SELECT count(*) as count FROM `cron_scheduler`;")
            .fetch_one(tx)
            .await
            .map_err(UiEventHandlerError::DBError)
            .context("error in count_list".to_string())
    }
}

pub struct CronSchedulerRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    pool: &'static RdbPool,
}

pub trait UseCronSchedulerRepository {
    fn cron_scheduler_repository(&self) -> &CronSchedulerRepositoryImpl;
}

impl CronSchedulerRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { id_generator, pool }
    }
}

impl UseRdbPool for CronSchedulerRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for CronSchedulerRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl CronSchedulerRepository for CronSchedulerRepositoryImpl {}

mod test {
    use super::CronSchedulerRepository;
    use super::CronSchedulerRepositoryImpl;
    use crate::infra::IdGeneratorWrapper;
    use crate::infra::UseIdGenerator;
    use anyhow::Context;
    use anyhow::Result;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::rdb::UseRdbPool;
    use proto::jobworkerp_conductor::data::cron_scheduler_data::ExecutionTarget;
    use proto::jobworkerp_conductor::data::CronSchedulerData;
    use proto::jobworkerp_conductor::data::WorkerExecution;
    use proto::jobworkerp_conductor::data::WorkflowExecution;

    async fn _test_repository(pool: &'static RdbPool) -> Result<()> {
        let repository = CronSchedulerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
        let db = repository.db_pool();

        // Test 1: URL execution mode (via execution_target oneof)
        let data = Some(CronSchedulerData {
            name: "hoge1".to_string(),
            jobworkerp_server_id: Some(proto::jobworkerp_conductor::data::JobworkerpServerId {
                value: 3,
            }),
            workflow_url: String::new(), // deprecated field
            channel: None,               // deprecated field
            crontab: "hoge5".to_string(),
            enabled: true,
            description: Some("hoge7".to_string()),
            created_at: 0,
            updated_at: 0,
            args: Some(r#"{"test": "value"}"#.to_string()),
            execution_target: Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "hoge3".to_string(),
                channel: Some("hoge4".to_string()),
            })),
        });

        let mut tx = db.begin().await.context("error in test")?;
        let id = repository.create(&mut *tx, &data.clone().unwrap()).await?;
        assert!(id.value > 0);
        tx.commit().await.context("error in test delete commit")?;

        let id1 = id;

        // find and verify execution_target
        let found = repository.find(&id1).await?;
        let found_data = found.as_ref().unwrap().data.as_ref().unwrap();
        assert!(matches!(
            &found_data.execution_target,
            Some(ExecutionTarget::Workflow(wf)) if wf.workflow_url == "hoge3"
        ));

        // Test 2: Update to worker execution mode
        tx = db.begin().await.context("error in test")?;
        let update = CronSchedulerData {
            name: "fuga1".to_string(),
            jobworkerp_server_id: Some(proto::jobworkerp_conductor::data::JobworkerpServerId {
                value: 4,
            }),
            workflow_url: String::new(),
            channel: None,
            crontab: "fuga5".to_string(),
            enabled: false,
            description: Some("fuga7".to_string()),
            created_at: 0,
            updated_at: 0,
            args: Some(r#"{"updated": "args"}"#.to_string()),
            execution_target: Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: "my-worker".to_string(),
                r#using: Some("chat".to_string()),
            })),
        };
        let updated = repository.update(&mut *tx, &id1, &update).await?;
        assert!(updated);
        tx.commit().await.context("error in test delete commit")?;

        // find and verify worker execution_target
        let found = repository.find(&id1).await?;
        let found_data = found.as_ref().unwrap().data.as_ref().unwrap();
        assert!(matches!(
            &found_data.execution_target,
            Some(ExecutionTarget::Worker(w)) if w.worker_name == "my-worker" && w.r#using == Some("chat".to_string())
        ));

        let count = repository.count_list_tx(repository.db_pool()).await?;
        assert_eq!(1, count);

        // delete record
        tx = db.begin().await.context("error in test")?;
        let del = repository.delete_tx(&mut *tx, &id1).await?;
        tx.commit().await.context("error in test delete commit")?;
        assert!(del, "delete error");

        // Test 3: Worker execution mode without using (single-method runner)
        let worker_data = CronSchedulerData {
            name: "worker_only".to_string(),
            jobworkerp_server_id: Some(proto::jobworkerp_conductor::data::JobworkerpServerId {
                value: 5,
            }),
            workflow_url: String::new(),
            channel: None,
            crontab: "0 * * * *".to_string(),
            enabled: true,
            description: None,
            created_at: 0,
            updated_at: 0,
            args: None,
            execution_target: Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: "cleanup-command".to_string(),
                r#using: None,
            })),
        };

        tx = db.begin().await.context("error in test")?;
        let id2 = repository.create(&mut *tx, &worker_data).await?;
        tx.commit().await.context("error in test commit")?;

        let found = repository.find(&id2).await?;
        let found_data = found.as_ref().unwrap().data.as_ref().unwrap();
        assert!(matches!(
            &found_data.execution_target,
            Some(ExecutionTarget::Worker(w)) if w.worker_name == "cleanup-command" && w.r#using.is_none()
        ));

        // cleanup
        tx = db.begin().await.context("error in test")?;
        repository.delete_tx(&mut *tx, &id2).await?;
        tx.commit().await.context("error in test commit")?;

        // Test 4: Legacy fallback (execution_target=None, deprecated workflow_url non-empty)
        // Simulate old data inserted without execution_target
        tx = db.begin().await.context("error in test")?;
        let legacy_id: i64 = repository.id_generator().generate_id()?;
        sqlx::query::<infra_utils::infra::rdb::Rdb>(
            "INSERT INTO `cron_scheduler` (
                `id`, `name`, `jobworkerp_server_id`, `workflow_url`, `channel`,
                `crontab`, `enabled`, `description`, `created_at`, `updated_at`, `args`
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(legacy_id)
        .bind("legacy_scheduler")
        .bind(1i64)
        .bind("https://legacy.example.com/workflow.yml")
        .bind(Some("legacy-channel"))
        .bind("0 * * * *")
        .bind(true)
        .bind(None::<String>)
        .bind(0i64)
        .bind(0i64)
        .bind(None::<String>)
        .execute(&mut *tx)
        .await?;
        tx.commit().await.context("error in test commit")?;

        let legacy_cid = proto::jobworkerp_conductor::data::CronSchedulerId { value: legacy_id };
        let found = repository.find(&legacy_cid).await?;
        let found_data = found.as_ref().unwrap().data.as_ref().unwrap();
        // to_proto() should construct WorkflowExecution from deprecated fields
        assert!(matches!(
            &found_data.execution_target,
            Some(ExecutionTarget::Workflow(wf)) if wf.workflow_url == "https://legacy.example.com/workflow.yml"
                && wf.channel == Some("legacy-channel".to_string())
        ));

        // cleanup
        tx = db.begin().await.context("error in test")?;
        repository.delete_tx(&mut *tx, &legacy_cid).await?;
        tx.commit().await.context("error in test commit")?;

        Ok(())
    }

    #[test]
    fn run_test() -> Result<()> {
        use infra_utils::infra::test::setup_test_rdb_from;
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = if cfg!(feature = "mysql") {
                let pool = setup_test_rdb_from("sql/mysql").await;
                sqlx::query("TRUNCATE TABLE cron_scheduler;")
                    .execute(pool)
                    .await?;
                pool
            } else {
                let pool = setup_test_rdb_from("sql/sqlite").await;
                sqlx::query("DELETE FROM cron_scheduler;")
                    .execute(pool)
                    .await?;
                pool
            };
            _test_repository(rdb_pool).await
        })
    }
}
