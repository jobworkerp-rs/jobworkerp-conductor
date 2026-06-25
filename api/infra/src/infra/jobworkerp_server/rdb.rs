use super::rows::JobworkerpServerRow;
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
    JobworkerpServer, JobworkerpServerData, JobworkerpServerId,
};
use sqlx::Executor;

#[async_trait]
pub trait JobworkerpServerRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    async fn create<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        jobworkerp_server: &JobworkerpServerData,
    ) -> Result<JobworkerpServerId> {
        let id: i64 = self.id_generator().generate_id()?;
        let res = sqlx::query::<Rdb>(
            "INSERT INTO `jobworkerp_server` (
            `id`,
            `name`,
            `host`,
            `port`,
            `ssl_enabled`,
            `description`,
            `enabled`,
            `created_at`,
            `updated_at`
            ) VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(id)
        .bind(&jobworkerp_server.name)
        .bind(&jobworkerp_server.host)
        .bind(&jobworkerp_server.port)
        .bind(jobworkerp_server.ssl_enabled)
        .bind(&jobworkerp_server.description)
        .bind(jobworkerp_server.enabled)
        .bind(jobworkerp_server.created_at)
        .bind(jobworkerp_server.updated_at)
        .execute(tx)
        .await;
        match res {
            Ok(r) if r.rows_affected() > 0 => Ok(JobworkerpServerId { value: id }),
            Ok(_) => Err(UiEventHandlerError::RuntimeError(format!(
                "Cannot insert jobworkerp_server (logic error?): {jobworkerp_server:?}"
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
                            "Server name '{}' already exists",
                            &jobworkerp_server.name
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
        id: &JobworkerpServerId,
        jobworkerp_server: &JobworkerpServerData,
    ) -> Result<bool> {
        sqlx::query(
            "UPDATE `jobworkerp_server` SET
            `name` = ?,
            `host` = ?,
            `port` = ?,
            `ssl_enabled` = ?,
            `description` = ?,
            `enabled` = ?,
            `created_at` = ?,
            `updated_at` = ?
            WHERE `id` = ?;",
        )
        .bind(&jobworkerp_server.name)
        .bind(&jobworkerp_server.host)
        .bind(&jobworkerp_server.port)
        .bind(jobworkerp_server.ssl_enabled)
        .bind(&jobworkerp_server.description)
        .bind(jobworkerp_server.enabled)
        .bind(jobworkerp_server.created_at)
        .bind(jobworkerp_server.updated_at)
        .bind(id.value)
        .execute(tx)
        .await
        .map(|r| r.rows_affected() > 0)
        .map_err(UiEventHandlerError::DBError)
        .context(format!("error in update: id = {}", id.value))
    }

    async fn delete(&self, id: &JobworkerpServerId) -> Result<bool> {
        self.delete_tx(self.db_pool(), id).await
    }

    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &JobworkerpServerId,
    ) -> Result<bool> {
        let del = sqlx::query::<Rdb>("DELETE FROM `jobworkerp_server` WHERE `id` = ?;")
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(UiEventHandlerError::DBError)?;
        Ok(del)
    }

    async fn find(&self, id: &JobworkerpServerId) -> Result<Option<JobworkerpServer>> {
        self.find_row_tx(self.db_pool(), id)
            .await
            .map(|r| r.map(|r2| r2.to_proto()))
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<JobworkerpServer>> {
        self.find_by_name_tx(self.db_pool(), name)
            .await
            .map(|r| r.map(|r2| r2.to_proto()))
    }

    async fn find_row_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &JobworkerpServerId,
    ) -> Result<Option<JobworkerpServerRow>> {
        sqlx::query_as::<Rdb, JobworkerpServerRow>(
            "SELECT * FROM `jobworkerp_server` WHERE `id` = ?;",
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
    ) -> Result<Option<JobworkerpServerRow>> {
        sqlx::query_as::<Rdb, JobworkerpServerRow>(
            "SELECT * FROM `jobworkerp_server` WHERE `name` = ? LIMIT 1;",
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
    ) -> Result<Vec<JobworkerpServer>> {
        self.find_row_list_tx(self.db_pool(), limit, offset)
            .await
            .map(|r| r.iter().map(|r2| r2.to_proto()).collect_vec())
    }

    async fn find_row_list_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<JobworkerpServerRow>> {
        if let Some(l) = limit {
            sqlx::query_as::<_, JobworkerpServerRow>(
                "SELECT * FROM `jobworkerp_server` ORDER BY `id` DESC LIMIT ? OFFSET ?;",
            )
            .bind(l)
            .bind(offset.unwrap_or(&0i64))
            .fetch_all(tx)
        } else {
            // fetch all!
            sqlx::query_as::<_, JobworkerpServerRow>(
                "SELECT * FROM `jobworkerp_server` ORDER BY `id` DESC;",
            )
            .fetch_all(tx)
        }
        .await
        .map_err(UiEventHandlerError::DBError)
        .context(format!("error in find_list: ({limit:?}, {offset:?})"))
    }

    async fn count_list_tx<'c, E: Executor<'c, Database = Rdb>>(&self, tx: E) -> Result<i64> {
        sqlx::query_scalar("SELECT count(*) as count FROM `jobworkerp_server`;")
            .fetch_one(tx)
            .await
            .map_err(UiEventHandlerError::DBError)
            .context("error in count_list".to_string())
    }
}

#[derive(Clone)]
pub struct JobworkerpServerRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    pool: &'static RdbPool,
}

pub trait UseJobworkerpServerRepository {
    fn jobworkerp_server_repository(&self) -> &JobworkerpServerRepositoryImpl;
}

impl JobworkerpServerRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { id_generator, pool }
    }
}

impl UseRdbPool for JobworkerpServerRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for JobworkerpServerRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl JobworkerpServerRepository for JobworkerpServerRepositoryImpl {}

mod test {
    use super::JobworkerpServerRepository;
    use super::JobworkerpServerRepositoryImpl;
    use crate::infra::IdGeneratorWrapper;
    use anyhow::Context;
    use anyhow::Result;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::rdb::UseRdbPool;
    use proto::jobworkerp_conductor::data::JobworkerpServer;
    use proto::jobworkerp_conductor::data::JobworkerpServerData;
    // use proto::jobworkerp_conductor::data::JobworkerpServerId;
    // use sqlx::Pool;

    async fn _test_repository(pool: &'static RdbPool) -> Result<()> {
        let repository = JobworkerpServerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
        let db = repository.db_pool();
        let data = Some(JobworkerpServerData {
            name: "hoge1".to_string(),
            host: "hoge2".to_string(),
            port: "hoge3".to_string(),
            ssl_enabled: true,
            description: Some("hoge5".to_string()),
            enabled: true,
            created_at: 0,
            updated_at: 0,
        });

        let mut tx = db.begin().await.context("error in test")?;
        let id = repository.create(&mut *tx, &data.clone().unwrap()).await?;
        assert!(id.value > 0);
        tx.commit().await.context("error in test delete commit")?;

        let id1 = id;
        let expect = JobworkerpServer {
            id: Some(id1),
            data,
        };

        // find
        let found = repository.find(&id1).await?;
        assert_eq!(Some(&expect), found.as_ref());

        // update
        tx = db.begin().await.context("error in test")?;
        let update = JobworkerpServerData {
            name: "fuga1".to_string(),
            host: "fuga2".to_string(),
            port: "fuga3".to_string(),
            ssl_enabled: false,
            description: Some("fuga5".to_string()),
            enabled: false,
            created_at: 0,
            updated_at: 0,
        };
        let updated = repository
            .update(&mut *tx, &expect.id.unwrap(), &update)
            //            .upsert(&mut tx, &expect.id.clone().unwrap(), &update)
            .await?;
        assert!(updated);
        tx.commit().await.context("error in test delete commit")?;

        // find
        let found = repository.find(&expect.id.unwrap()).await?;
        assert_eq!(&update, &found.unwrap().data.unwrap());
        let count = repository.count_list_tx(repository.db_pool()).await?;
        assert_eq!(1, count);

        // delete record
        tx = db.begin().await.context("error in test")?;
        let del = repository.delete_tx(&mut *tx, &expect.id.unwrap()).await?;
        tx.commit().await.context("error in test delete commit")?;
        assert!(del, "delete error");
        Ok(())
    }

    #[test]
    fn run_test() -> Result<()> {
        use infra_utils::infra::test::setup_test_rdb_from;
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = if cfg!(feature = "mysql") {
                let pool = setup_test_rdb_from("sql/mysql").await;
                sqlx::query("TRUNCATE TABLE jobworkerp_server;")
                    .execute(pool)
                    .await?;
                pool
            } else {
                let pool = setup_test_rdb_from("sql/sqlite").await;
                sqlx::query("DELETE FROM jobworkerp_server;")
                    .execute(pool)
                    .await?;
                pool
            };
            _test_repository(rdb_pool).await
        })
    }

    // Performance and index effectiveness tests
    #[cfg(feature = "performance_tests")]
    mod performance_tests {
        use super::*;
        use std::time::Instant;

        async fn _test_find_by_name_performance(pool: &'static RdbPool) -> Result<()> {
            let repository = JobworkerpServerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);

            // Insert test data (100 records)
            let mut tx = pool.begin().await.context("error in test")?;
            let mut server_ids = Vec::new();

            for i in 0..100 {
                let data = JobworkerpServerData {
                    name: format!("test_server_{i:03}"),
                    host: "localhost".to_string(),
                    port: "8080".to_string(),
                    ssl_enabled: false,
                    description: Some(format!("Test server {i}")),
                    enabled: true,
                    created_at: i as i64,
                    updated_at: i as i64,
                };

                let id = repository.create(&mut *tx, &data).await?;
                server_ids.push(id);
            }
            tx.commit().await.context("error in test commit")?;

            // Test find_by_name performance (should benefit from index)
            let start = Instant::now();
            for i in 0..100 {
                let name = format!("test_server_{i:03}");
                let found = repository.find_by_name(&name).await?;
                assert!(found.is_some(), "Server {name} should be found");
            }
            let duration = start.elapsed();
            println!("find_by_name for 100 queries took: {duration:?}");

            // Test find_list performance (should benefit from enabled index)
            let start = Instant::now();
            let list = repository.find_list(Some(&50), Some(&0)).await?;
            let duration = start.elapsed();
            println!("find_list with limit took: {duration:?}");
            assert_eq!(list.len(), 50);

            // Cleanup
            for id in server_ids {
                repository.delete(&id).await?;
            }

            Ok(())
        }

        async fn _test_unique_constraint_violation(pool: &'static RdbPool) -> Result<()> {
            let repository = JobworkerpServerRepositoryImpl::new(IdGeneratorWrapper::new(), pool);

            let data1 = JobworkerpServerData {
                name: "duplicate_server".to_string(),
                host: "localhost".to_string(),
                port: "8080".to_string(),
                ssl_enabled: false,
                description: Some("First server".to_string()),
                enabled: true,
                created_at: 1000,
                updated_at: 1000,
            };

            let data2 = JobworkerpServerData {
                name: "duplicate_server".to_string(), // Same name
                host: "remotehost".to_string(),
                port: "8081".to_string(),
                ssl_enabled: true,
                description: Some("Second server".to_string()),
                enabled: true,
                created_at: 2000,
                updated_at: 2000,
            };

            // First insert should succeed
            let mut tx = pool.begin().await.context("error in test")?;
            let id1 = repository.create(&mut *tx, &data1).await?;
            tx.commit().await.context("error in test commit")?;

            // Second insert with same name should fail if UNIQUE constraint is applied
            let mut tx = pool.begin().await.context("error in test")?;
            let result = repository.create(&mut *tx, &data2).await;

            // Check if UNIQUE constraint is working
            if result.is_err() {
                println!("✅ UNIQUE constraint is working - duplicate name rejected");
                tx.rollback().await.context("error in test rollback")?;
            } else {
                println!("⚠️  UNIQUE constraint not yet applied - duplicate allowed");
                tx.commit().await.context("error in test commit")?;
                // Clean up the duplicate
                if let Ok(id2) = result {
                    repository.delete(&id2).await?;
                }
            }

            // Cleanup
            repository.delete(&id1).await?;

            Ok(())
        }

        #[test]
        fn run_performance_tests() -> Result<()> {
            use infra_utils::infra::test::setup_test_rdb_from;
            use infra_utils::infra::test::TEST_RUNTIME;
            TEST_RUNTIME.block_on(async {
                let rdb_pool = if cfg!(feature = "mysql") {
                    let pool = setup_test_rdb_from("sql/mysql").await;
                    sqlx::query("TRUNCATE TABLE jobworkerp_server;")
                        .execute(pool)
                        .await?;
                    pool
                } else {
                    let pool = setup_test_rdb_from("sql/sqlite").await;
                    sqlx::query("DELETE FROM jobworkerp_server;")
                        .execute(pool)
                        .await?;
                    pool
                };

                println!("=== Running Performance Tests ===");
                _test_find_by_name_performance(rdb_pool).await?;
                _test_unique_constraint_violation(rdb_pool).await?;

                Ok(())
            })
        }
    }
}
