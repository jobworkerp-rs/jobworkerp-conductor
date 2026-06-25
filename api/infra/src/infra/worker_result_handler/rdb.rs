use super::rows::WorkerResultHandlerRow;
use crate::error::UiEventHandlerError;
use crate::infra::IdGeneratorWrapper;
use crate::infra::UseIdGenerator;
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use itertools::Itertools;
use proto::jobworkerp_conductor::data::{
    worker_result_handler_data::ExecutionTarget, WorkerResultHandler, WorkerResultHandlerData,
    WorkerResultHandlerId,
};
use sqlx::Executor;

/// Extract flat DB column values from oneof execution_target.
/// Returns (workflow_url, channel, worker_name, using).
/// Note: channel is Option<String> because WorkerResultHandlerData.channel is an optional proto field
/// (unlike SlackEventHandlerData where channel is required/String).
fn flatten_execution_target(
    data: &WorkerResultHandlerData,
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
            // Fallback to deprecated fields 5/6
            (data.workflow_url.clone(), data.channel.clone(), None, None)
        }
    }
}

#[async_trait]
pub trait WorkerResultHandlerRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    async fn create<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        worker_result_handler: &WorkerResultHandlerData,
    ) -> Result<WorkerResultHandlerId> {
        let id: i64 = self.id_generator().generate_id()?;
        let (db_workflow_url, db_channel, db_worker_name, db_using) =
            flatten_execution_target(worker_result_handler);
        let res = sqlx::query::<Rdb>(
            "INSERT INTO `worker_result_handler` (
            `id`,
            `name`,
            `listen_jobworkerp_server_id`,
            `listen_worker_name`,
            `process_jobworkerp_server_id`,
            `workflow_url`,
            `channel`,
            `enabled`,
            `description`,
            `created_at`,
            `updated_at`,
            `args`,
            `worker_name`,
            `using`
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(id)
        .bind(&worker_result_handler.name)
        .bind(
            worker_result_handler
                .listen_jobworkerp_server_id
                .as_ref()
                .map(|id| id.value),
        )
        .bind(&worker_result_handler.listen_worker_name)
        .bind(
            worker_result_handler
                .process_jobworkerp_server_id
                .as_ref()
                .map(|id| id.value),
        )
        .bind(&db_workflow_url)
        .bind(&db_channel)
        .bind(worker_result_handler.enabled)
        .bind(&worker_result_handler.description)
        .bind(worker_result_handler.created_at)
        .bind(worker_result_handler.updated_at)
        .bind(&worker_result_handler.args)
        .bind(&db_worker_name)
        .bind(&db_using)
        .execute(tx)
        .await;
        match res {
            Ok(r) if r.rows_affected() > 0 => Ok(WorkerResultHandlerId { value: id }),
            Ok(_) => Err(UiEventHandlerError::RuntimeError(format!(
                "Cannot insert worker_result_handler (logic error?): {worker_result_handler:?}"
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
                            "WorkerResultHandler name '{}' already exists",
                            &worker_result_handler.name
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
        id: &WorkerResultHandlerId,
        worker_result_handler: &WorkerResultHandlerData,
    ) -> Result<bool> {
        let (db_workflow_url, db_channel, db_worker_name, db_using) =
            flatten_execution_target(worker_result_handler);
        sqlx::query(
            "UPDATE `worker_result_handler` SET
            `name` = ?,
            `listen_jobworkerp_server_id` = ?,
            `listen_worker_name` = ?,
            `process_jobworkerp_server_id` = ?,
            `workflow_url` = ?,
            `channel` = ?,
            `enabled` = ?,
            `description` = ?,
            `created_at` = ?,
            `updated_at` = ?,
            `args` = ?,
            `worker_name` = ?,
            `using` = ?
            WHERE `id` = ?;",
        )
        .bind(&worker_result_handler.name)
        .bind(
            worker_result_handler
                .listen_jobworkerp_server_id
                .as_ref()
                .map(|id| id.value),
        )
        .bind(&worker_result_handler.listen_worker_name)
        .bind(
            worker_result_handler
                .process_jobworkerp_server_id
                .as_ref()
                .map(|id| id.value),
        )
        .bind(&db_workflow_url)
        .bind(&db_channel)
        .bind(worker_result_handler.enabled)
        .bind(&worker_result_handler.description)
        .bind(worker_result_handler.created_at)
        .bind(worker_result_handler.updated_at)
        .bind(&worker_result_handler.args)
        .bind(&db_worker_name)
        .bind(&db_using)
        .bind(id.value)
        .execute(tx)
        .await
        .map(|r| r.rows_affected() > 0)
        .map_err(UiEventHandlerError::DBError)
        .context(format!("error in update: id = {}", id.value))
    }

    async fn delete(&self, id: &WorkerResultHandlerId) -> Result<bool> {
        self.delete_tx(self.db_pool(), id).await
    }

    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &WorkerResultHandlerId,
    ) -> Result<bool> {
        let del = sqlx::query::<Rdb>("DELETE FROM `worker_result_handler` WHERE `id` = ?;")
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(UiEventHandlerError::DBError)?;
        Ok(del)
    }

    async fn find(&self, id: &WorkerResultHandlerId) -> Result<Option<WorkerResultHandler>> {
        self.find_row_tx(self.db_pool(), id)
            .await
            .map(|r| r.map(|r2| r2.to_proto()))
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<WorkerResultHandler>> {
        self.find_by_name_tx(self.db_pool(), name)
            .await
            .map(|r| r.map(|r2| r2.to_proto()))
    }

    async fn find_row_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &WorkerResultHandlerId,
    ) -> Result<Option<WorkerResultHandlerRow>> {
        sqlx::query_as::<Rdb, WorkerResultHandlerRow>(
            "SELECT * FROM `worker_result_handler` WHERE `id` = ?;",
        )
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
    ) -> Result<Option<WorkerResultHandlerRow>> {
        sqlx::query_as::<Rdb, WorkerResultHandlerRow>(
            "SELECT * FROM `worker_result_handler` WHERE `name` = ? LIMIT 1;",
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
    ) -> Result<Vec<WorkerResultHandler>> {
        self.find_row_list_tx(self.db_pool(), limit, offset)
            .await
            .map(|r| r.iter().map(|r2| r2.to_proto()).collect_vec())
    }

    async fn find_row_list_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<WorkerResultHandlerRow>> {
        if let Some(l) = limit {
            sqlx::query_as::<_, WorkerResultHandlerRow>(
                "SELECT * FROM `worker_result_handler` ORDER BY `id` DESC LIMIT ? OFFSET ?;",
            )
            .bind(l)
            .bind(offset.unwrap_or(&0i64))
            .fetch_all(tx)
        } else {
            // fetch all!
            sqlx::query_as::<_, WorkerResultHandlerRow>(
                "SELECT * FROM `worker_result_handler` ORDER BY `id` DESC;",
            )
            .fetch_all(tx)
        }
        .await
        .map_err(UiEventHandlerError::DBError)
        .context(format!("error in find_list: ({limit:?}, {offset:?})"))
    }

    async fn count_list_tx<'c, E: Executor<'c, Database = Rdb>>(&self, tx: E) -> Result<i64> {
        sqlx::query_scalar("SELECT count(*) as count FROM `worker_result_handler`;")
            .fetch_one(tx)
            .await
            .map_err(UiEventHandlerError::DBError)
            .context("error in count_list".to_string())
    }
}

pub struct WorkerResultHandlerRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    pool: &'static RdbPool,
}

pub trait UseWorkerResultHandlerRepository {
    fn worker_result_handler_repository(&self) -> &WorkerResultHandlerRepositoryImpl;
}

impl WorkerResultHandlerRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { id_generator, pool }
    }
}

impl UseRdbPool for WorkerResultHandlerRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for WorkerResultHandlerRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl WorkerResultHandlerRepository for WorkerResultHandlerRepositoryImpl {}

mod test {
    use std::time::SystemTime;

    use super::WorkerResultHandlerRepository;
    use super::WorkerResultHandlerRepositoryImpl;
    use crate::infra::IdGeneratorWrapper;
    use anyhow::Context;
    use anyhow::Result;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::rdb::UseRdbPool;
    use proto::jobworkerp_conductor::data::worker_result_handler_data::ExecutionTarget;
    use proto::jobworkerp_conductor::data::WorkerResultHandler;
    use proto::jobworkerp_conductor::data::WorkerResultHandlerData;
    use proto::jobworkerp_conductor::data::WorkflowExecution;

    async fn _test_repository(pool: &'static RdbPool) -> Result<()> {
        let repository = WorkerResultHandlerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
        let db = repository.db_pool();
        let data = Some(WorkerResultHandlerData {
            name: "hoge1".to_string(),
            listen_jobworkerp_server_id: Some(
                proto::jobworkerp_conductor::data::JobworkerpServerId { value: 3 },
            ),
            listen_worker_name: "hoge3".to_string(),
            process_jobworkerp_server_id: Some(
                proto::jobworkerp_conductor::data::JobworkerpServerId { value: 5 },
            ),
            workflow_url: "hoge5".to_string(),
            channel: Some("hoge6".to_string()),
            enabled: true,
            description: Some("hoge8".to_string()),
            created_at: SystemTime::now().elapsed().unwrap().as_secs() as i64,
            updated_at: SystemTime::now().elapsed().unwrap().as_secs() as i64,
            args: Some(r#"{"worker": "args"}"#.to_string()),
            execution_target: None,
        });

        let mut tx = db.begin().await.context("error in test")?;
        let id = repository.create(&mut *tx, &data.clone().unwrap()).await?;
        assert!(id.value > 0);
        tx.commit().await.context("error in test delete commit")?;

        let id1 = id;
        // to_proto() builds execution_target from workflow_url/channel when execution_target is None
        let mut expect_data = data.clone().unwrap();
        expect_data.execution_target = Some(ExecutionTarget::Workflow(WorkflowExecution {
            workflow_url: "hoge5".to_string(),
            channel: Some("hoge6".to_string()),
        }));
        let expect = WorkerResultHandler {
            id: Some(id1),
            data: Some(expect_data),
        };

        // find
        let found = repository.find(&id1).await?;
        assert_eq!(Some(&expect), found.as_ref());

        // update
        tx = db.begin().await.context("error in test")?;
        let update = WorkerResultHandlerData {
            name: "fuga1".to_string(),
            listen_jobworkerp_server_id: Some(
                proto::jobworkerp_conductor::data::JobworkerpServerId { value: 4 },
            ),
            listen_worker_name: "fuga3".to_string(),
            process_jobworkerp_server_id: Some(
                proto::jobworkerp_conductor::data::JobworkerpServerId { value: 6 },
            ),
            workflow_url: "fuga5".to_string(),
            channel: Some("fuga6".to_string()),
            enabled: false,
            description: Some("fuga8".to_string()),
            created_at: expect.data.as_ref().unwrap().created_at,
            updated_at: expect.data.as_ref().unwrap().updated_at, // update time
            args: Some(r#"{"updated": "worker_args"}"#.to_string()),
            execution_target: None,
        };
        let updated = repository
            .update(&mut *tx, &expect.id.unwrap(), &update)
            //            .upsert(&mut tx, &expect.id.clone().unwrap(), &update)
            .await?;
        assert!(updated);
        tx.commit().await.context("error in test delete commit")?;

        // find: to_proto() builds execution_target from workflow_url/channel
        let found = repository.find(&expect.id.unwrap()).await?;
        let mut expected_update = update.clone();
        expected_update.execution_target = Some(ExecutionTarget::Workflow(WorkflowExecution {
            workflow_url: "fuga5".to_string(),
            channel: Some("fuga6".to_string()),
        }));
        assert_eq!(&expected_update, &found.unwrap().data.unwrap());
        let count = repository.count_list_tx(repository.db_pool()).await?;
        assert_eq!(1, count);

        // delete record
        tx = db.begin().await.context("error in test")?;
        let del = repository.delete_tx(&mut *tx, &expect.id.unwrap()).await?;
        tx.commit().await.context("error in test delete commit")?;
        assert!(del, "delete error");
        Ok(())
    }

    async fn _test_worker_execution_mode(pool: &'static RdbPool) -> Result<()> {
        use proto::jobworkerp_conductor::data::WorkerExecution;

        let repository = WorkerResultHandlerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
        let db = repository.db_pool();

        // Create with worker execution mode
        let data = WorkerResultHandlerData {
            name: "worker_mode_test".to_string(),
            listen_jobworkerp_server_id: Some(
                proto::jobworkerp_conductor::data::JobworkerpServerId { value: 1 },
            ),
            listen_worker_name: "listen_worker".to_string(),
            process_jobworkerp_server_id: Some(
                proto::jobworkerp_conductor::data::JobworkerpServerId { value: 2 },
            ),
            workflow_url: String::new(),
            channel: None,
            enabled: true,
            description: Some("worker mode test".to_string()),
            created_at: 0,
            updated_at: 0,
            args: Some(r#"{"key": "value"}"#.to_string()),
            execution_target: Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: "my-worker".to_string(),
                using: Some("run".to_string()),
            })),
        };

        let mut tx = db.begin().await.context("begin")?;
        let id = repository.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;

        // Find and verify worker execution target is preserved
        let found = repository.find(&id).await?.expect("should find");
        let found_data = found.data.as_ref().unwrap();
        assert_eq!(
            found_data.execution_target,
            Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: "my-worker".to_string(),
                using: Some("run".to_string()),
            }))
        );
        // Deprecated fields should be empty for worker mode
        assert!(found_data.workflow_url.is_empty());
        assert!(found_data.channel.is_none());

        // Update: switch from worker to workflow mode
        let update_data = WorkerResultHandlerData {
            execution_target: Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "http://example.com/wf.yml".to_string(),
                channel: Some("ch1".to_string()),
            })),
            ..data.clone()
        };
        let mut tx = db.begin().await.context("begin")?;
        repository.update(&mut *tx, &id, &update_data).await?;
        tx.commit().await.context("commit")?;

        let found2 = repository
            .find(&id)
            .await?
            .expect("should find after update");
        let found2_data = found2.data.as_ref().unwrap();
        assert_eq!(
            found2_data.execution_target,
            Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "http://example.com/wf.yml".to_string(),
                channel: Some("ch1".to_string()),
            }))
        );

        // Cleanup
        let mut tx = db.begin().await.context("begin")?;
        repository.delete_tx(&mut *tx, &id).await?;
        tx.commit().await.context("commit")?;

        Ok(())
    }

    #[test]
    fn run_test() -> Result<()> {
        use infra_utils::infra::test::setup_test_rdb_from;
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = if cfg!(feature = "mysql") {
                let pool = setup_test_rdb_from("sql/mysql").await;
                sqlx::query("TRUNCATE TABLE worker_result_handler;")
                    .execute(pool)
                    .await?;
                pool
            } else {
                let pool = setup_test_rdb_from("sql/sqlite").await;
                sqlx::query("DELETE FROM worker_result_handler;")
                    .execute(pool)
                    .await?;
                pool
            };
            _test_repository(rdb_pool).await?;
            _test_worker_execution_mode(rdb_pool).await
        })
    }
}
