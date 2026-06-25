use super::rows::ExecutionRefRow;
use crate::error::UiEventHandlerError;
use crate::infra::{IdGeneratorWrapper, UseIdGenerator};
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use proto::jobworkerp_conductor::data::{ExecutionRef, ExecutionRefId, ExecutionSourceType};
use sqlx::{Executor, QueryBuilder};

/// `result_status` value for a cancelled job, mirroring jobworkerp's `ResultStatus::CANCELLED = 6`.
/// Stored as a bare i32 because this layer has no jobworkerp-client dependency; the conductor app
/// layer records this exact value on a successful cancel (see `cancel_execution`).
const RESULT_STATUS_CANCELLED: i32 = 6;

/// Max ids bound in a single `IN (...)` delete. A storage-layer constraint: it stays under SQLite's
/// historical 999-parameter default and well under MySQL's placeholder/packet limits, so a large id
/// list is deleted in several statements instead of failing the bind-parameter limit. Shared by
/// `delete_by_ids` and its test so the threshold has a single owner.
const DELETE_CHUNK_SIZE: usize = 900;

/// Filter for the cross-source execution_ref listing (`find_list` / `count_list`). All fields are
/// optional and AND-combined; `None` means "no constraint on this column". Status is intentionally
/// absent: `resolved_status` is computed at read time against jobworkerp and is not stored, so it
/// cannot be filtered in SQL (see plan §6.3).
#[derive(Debug, Default, Clone)]
pub struct ExecutionRefListFilter {
    pub source_type: Option<i32>,
    pub jobworkerp_server_id: Option<i64>,
    pub triggered_after: Option<i64>,
    pub triggered_before: Option<i64>,
}

impl ExecutionRefListFilter {
    /// Append the dynamic `WHERE` clause for this filter onto a QueryBuilder. Emits ` WHERE ` for
    /// the first predicate and ` AND ` for subsequent ones; emits nothing when no field is set.
    fn push_where(&self, qb: &mut QueryBuilder<Rdb>) {
        let mut first = true;
        let mut clause = |qb: &mut QueryBuilder<Rdb>| {
            qb.push(if first { " WHERE " } else { " AND " });
            first = false;
        };
        if let Some(st) = self.source_type {
            clause(qb);
            qb.push("`source_type` = ").push_bind(st);
        }
        if let Some(sid) = self.jobworkerp_server_id {
            clause(qb);
            qb.push("`jobworkerp_server_id` = ").push_bind(sid);
        }
        if let Some(after) = self.triggered_after {
            clause(qb);
            qb.push("`triggered_at` >= ").push_bind(after);
        }
        if let Some(before) = self.triggered_before {
            clause(qb);
            qb.push("`triggered_at` <= ").push_bind(before);
        }
    }
}

#[async_trait]
pub trait ExecutionRefRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    async fn create<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        execution_ref: &ExecutionRef,
    ) -> Result<ExecutionRefId> {
        let id = self.id_generator().generate_id()?;
        let server_id = execution_ref.jobworkerp_server_id.as_ref().ok_or_else(|| {
            UiEventHandlerError::RuntimeError("jobworkerp_server_id is required".to_string())
        })?;
        let now = if execution_ref.created_at > 0 {
            execution_ref.created_at
        } else {
            chrono::Utc::now().timestamp()
        };
        let triggered_at = if execution_ref.triggered_at > 0 {
            execution_ref.triggered_at
        } else {
            now
        };

        let res = sqlx::query::<Rdb>(
            "INSERT INTO `execution_ref` (
                `id`, `source_type`, `source_id`, `source_name`, `jobworkerp_server_id`,
                `job_id`, `triggered_at`, `trigger_context_json`, `enqueue_error`, `created_at`,
                `result_status`
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(execution_ref.source_type)
        .bind(execution_ref.source_id)
        .bind(&execution_ref.source_name)
        .bind(server_id.value)
        .bind(execution_ref.job_id)
        .bind(triggered_at)
        .bind(&execution_ref.trigger_context_json)
        .bind(&execution_ref.enqueue_error)
        .bind(now)
        .bind(execution_ref.result_status)
        .execute(tx)
        .await;

        match res {
            Ok(r) if r.rows_affected() > 0 => Ok(ExecutionRefId { value: id }),
            Ok(_) => Err(UiEventHandlerError::RuntimeError(format!(
                "Cannot insert execution_ref: {execution_ref:?}"
            ))
            .into()),
            Err(e) => Err(UiEventHandlerError::DBError(e).into()),
        }
    }

    /// Update the terminal outcome of an execution recorded at enqueue time.
    ///
    /// The pending row is created before the job reaches a terminal state (so the running job is
    /// trackable / cancellable); this fills in the assigned `job_id` and the observed terminal
    /// `result_status` once the job finishes. A zero `rows_affected` is logged but not treated as
    /// an error: recording is best-effort and must never fail the surrounding execution.
    ///
    /// A row already marked `Cancelled` is left untouched: cancelling a PENDING job deletes it
    /// without producing a JobResult, so the concurrently-awaited terminal future resolves to a
    /// JobResult-less Success (the streaming client treats a missing result as Success). Without
    /// this guard that late Success update would overwrite the recorded Cancelled and the status
    /// API would report a cancelled job as Succeeded. Cancelled is terminal and irreversible, so it
    /// must win regardless of which update lands last.
    async fn update_result<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &ExecutionRefId,
        job_id: Option<i64>,
        result_status: i32,
    ) -> Result<()> {
        let res = sqlx::query::<Rdb>(
            "UPDATE `execution_ref` SET `job_id` = ?, `result_status` = ?
             WHERE `id` = ? AND (`result_status` IS NULL OR `result_status` != ?)",
        )
        .bind(job_id)
        .bind(result_status)
        .bind(id.value)
        .bind(RESULT_STATUS_CANCELLED)
        .execute(tx)
        .await
        .map_err(UiEventHandlerError::DBError)?;
        if res.rows_affected() == 0 {
            // Either the id does not exist or the row is already Cancelled (a protected no-op).
            // Both are expected for this best-effort update, so log at debug rather than warn.
            tracing::debug!(
                "update_result affected no rows: execution_ref id={} not found or already cancelled",
                id.value
            );
        }
        Ok(())
    }

    /// Record the assigned job_id mid-flight (before the job reaches a terminal state), leaving
    /// `result_status` NULL so the status API reports the live processing status while the job
    /// runs. Best-effort like `update_result`.
    async fn update_job_id<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &ExecutionRefId,
        job_id: i64,
    ) -> Result<()> {
        let res = sqlx::query::<Rdb>("UPDATE `execution_ref` SET `job_id` = ? WHERE `id` = ?")
            .bind(job_id)
            .bind(id.value)
            .execute(tx)
            .await
            .map_err(UiEventHandlerError::DBError)?;
        if res.rows_affected() == 0 {
            tracing::warn!(
                "update_job_id affected no rows: execution_ref id={} not found",
                id.value
            );
        }
        Ok(())
    }

    /// Update an enqueue-time execution record that never reached a terminal job (connection
    /// failure, worker not found, etc.) with the enqueue error. Best-effort like `update_result`.
    async fn update_enqueue_error<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &ExecutionRefId,
        error: &str,
    ) -> Result<()> {
        let res =
            sqlx::query::<Rdb>("UPDATE `execution_ref` SET `enqueue_error` = ? WHERE `id` = ?")
                .bind(error)
                .bind(id.value)
                .execute(tx)
                .await
                .map_err(UiEventHandlerError::DBError)?;
        if res.rows_affected() == 0 {
            tracing::warn!(
                "update_enqueue_error affected no rows: execution_ref id={} not found",
                id.value
            );
        }
        Ok(())
    }

    async fn find(&self, id: &ExecutionRefId) -> Result<Option<ExecutionRef>> {
        let row: Option<ExecutionRefRow> =
            sqlx::query_as("SELECT * FROM `execution_ref` WHERE `id` = ?")
                .bind(id.value)
                .fetch_optional(self.db_pool())
                .await
                .context("Failed to find execution_ref by id")?;
        Ok(row.map(|r| r.to_proto()))
    }

    async fn find_latest_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
    ) -> Result<Option<ExecutionRef>> {
        let row: Option<ExecutionRefRow> = sqlx::query_as(
            "SELECT * FROM `execution_ref`
             WHERE `source_type` = ? AND `source_id` = ?
             ORDER BY `triggered_at` DESC, `id` DESC LIMIT 1",
        )
        .bind(source_type as i32)
        .bind(source_id)
        .fetch_optional(self.db_pool())
        .await
        .context("Failed to find latest execution_ref by source")?;
        Ok(row.map(|r| r.to_proto()))
    }

    async fn find_list_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<ExecutionRef>> {
        let rows: Vec<ExecutionRefRow> = match (limit, offset) {
            (Some(limit), Some(offset)) => {
                sqlx::query_as(
                    "SELECT * FROM `execution_ref`
                 WHERE `source_type` = ? AND `source_id` = ?
                 ORDER BY `triggered_at` DESC, `id` DESC LIMIT ? OFFSET ?",
                )
                .bind(source_type as i32)
                .bind(source_id)
                .bind(*limit)
                .bind(*offset)
                .fetch_all(self.db_pool())
                .await?
            }
            (Some(limit), None) => {
                sqlx::query_as(
                    "SELECT * FROM `execution_ref`
                 WHERE `source_type` = ? AND `source_id` = ?
                 ORDER BY `triggered_at` DESC, `id` DESC LIMIT ?",
                )
                .bind(source_type as i32)
                .bind(source_id)
                .bind(*limit)
                .fetch_all(self.db_pool())
                .await?
            }
            _ => {
                sqlx::query_as(
                    "SELECT * FROM `execution_ref`
                 WHERE `source_type` = ? AND `source_id` = ?
                 ORDER BY `triggered_at` DESC, `id` DESC",
                )
                .bind(source_type as i32)
                .bind(source_id)
                .fetch_all(self.db_pool())
                .await?
            }
        };
        Ok(rows.into_iter().map(|r| r.to_proto()).collect())
    }

    /// Delete a single execution_ref by id. Returns whether a row was actually removed (false for a
    /// missing id). The terminal-vs-active protection lives in the app layer (this repository has no
    /// jobworkerp connection to resolve runtime status), so this is an unconditional delete.
    async fn delete(&self, id: &ExecutionRefId) -> Result<bool> {
        let res = sqlx::query::<Rdb>("DELETE FROM `execution_ref` WHERE `id` = ?")
            .bind(id.value)
            .execute(self.db_pool())
            .await
            .map_err(UiEventHandlerError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Delete the execution_refs whose id is in `ids`, returning the number removed. The app layer
    /// resolves runtime status and passes only the ids it deems deletable (e.g. terminal ones), so
    /// the repository stays free of status-resolution responsibility (plan §5.4). Empty input is a
    /// no-op returning 0.
    ///
    /// The ids are deleted in chunks so a large list never exceeds the database bind-parameter limit
    /// (SQLite ~999/32766, MySQL packet/placeholder limits) — without chunking a long-running source
    /// could accumulate enough terminal refs to make the whole `IN (...)` delete fail. Each chunk is
    /// its own statement; chunks are independent, so a mid-way failure still surfaces as an error.
    async fn delete_by_ids(&self, ids: &[i64]) -> Result<u64> {
        let mut deleted = 0u64;
        for chunk in ids.chunks(DELETE_CHUNK_SIZE) {
            let mut qb = QueryBuilder::<Rdb>::new("DELETE FROM `execution_ref` WHERE `id` IN (");
            let mut separated = qb.separated(", ");
            for id in chunk {
                separated.push_bind(*id);
            }
            qb.push(")");
            let res = qb
                .build()
                .execute(self.db_pool())
                .await
                .map_err(UiEventHandlerError::DBError)?;
            deleted += res.rows_affected();
        }
        Ok(deleted)
    }

    /// Delete every execution_ref of a source unconditionally (force cleanup, used when
    /// `include_active=true`). Bypasses status resolution entirely so it works even when jobworkerp
    /// is unreachable.
    async fn delete_all_by_source(
        &self,
        source_type: ExecutionSourceType,
        source_id: i64,
    ) -> Result<u64> {
        let res = sqlx::query::<Rdb>(
            "DELETE FROM `execution_ref` WHERE `source_type` = ? AND `source_id` = ?",
        )
        .bind(source_type as i32)
        .bind(source_id)
        .execute(self.db_pool())
        .await
        .map_err(UiEventHandlerError::DBError)?;
        Ok(res.rows_affected())
    }

    /// Cross-source listing with a dynamic `WHERE` built from `filter`, ordered
    /// `triggered_at DESC, id DESC` (same as `find_list_by_source`). `offset` is only honored
    /// together with `limit` (SQLite rejects a bare OFFSET), matching the existing pagination
    /// convention.
    async fn find_list(
        &self,
        filter: &ExecutionRefListFilter,
        limit: Option<i32>,
        offset: Option<i64>,
    ) -> Result<Vec<ExecutionRef>> {
        let mut qb = QueryBuilder::<Rdb>::new("SELECT * FROM `execution_ref`");
        filter.push_where(&mut qb);
        qb.push(" ORDER BY `triggered_at` DESC, `id` DESC");
        if let Some(l) = limit {
            qb.push(" LIMIT ").push_bind(l);
            if let Some(o) = offset {
                qb.push(" OFFSET ").push_bind(o);
            }
        }
        let rows: Vec<ExecutionRefRow> = qb
            .build_query_as()
            .fetch_all(self.db_pool())
            .await
            .map_err(UiEventHandlerError::DBError)?;
        Ok(rows.into_iter().map(|r| r.to_proto()).collect())
    }

    /// Count of rows matching `filter` (same predicates as `find_list`), for pager totals.
    async fn count_list(&self, filter: &ExecutionRefListFilter) -> Result<i64> {
        let mut qb = QueryBuilder::<Rdb>::new("SELECT COUNT(*) FROM `execution_ref`");
        filter.push_where(&mut qb);
        let row: (i64,) = qb
            .build_query_as()
            .fetch_one(self.db_pool())
            .await
            .map_err(UiEventHandlerError::DBError)?;
        Ok(row.0)
    }
}

#[derive(Clone)]
pub struct ExecutionRefRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    pool: &'static RdbPool,
}

pub trait UseExecutionRefRepository {
    fn execution_ref_repository(&self) -> &ExecutionRefRepositoryImpl;
}

impl ExecutionRefRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { id_generator, pool }
    }
}

impl UseRdbPool for ExecutionRefRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ExecutionRefRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl ExecutionRefRepository for ExecutionRefRepositoryImpl {}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionRefListFilter, ExecutionRefRepository, ExecutionRefRepositoryImpl,
        DELETE_CHUNK_SIZE,
    };
    use crate::infra::IdGeneratorWrapper;
    use infra_utils::infra::rdb::{RdbPool, UseRdbPool};
    use proto::jobworkerp_conductor::data::{
        ExecutionRef, ExecutionRefId, ExecutionSourceType, JobworkerpServerId,
    };

    #[cfg(feature = "mysql")]
    const MYSQL_SCHEMA: &str = include_str!("../../../sql/mysql/002_schema.sql");
    #[cfg(feature = "mysql")]
    static MYSQL_POOL: tokio::sync::OnceCell<RdbPool> = tokio::sync::OnceCell::const_new();

    macro_rules! test_async {
        ($name:ident, $body:block) => {
            #[test]
            fn $name() {
                infra_utils::infra::test::TEST_RUNTIME.block_on(async $body);
            }
        };
    }

    #[cfg(not(feature = "mysql"))]
    async fn setup() -> ExecutionRefRepositoryImpl {
        let pool = Box::leak(Box::new(RdbPool::connect("sqlite::memory:").await.unwrap()));
        sqlx::query(
            "CREATE TABLE execution_ref (
                id BIGINT NOT NULL PRIMARY KEY,
                source_type INTEGER NOT NULL,
                source_id BIGINT NOT NULL,
                source_name TEXT NOT NULL,
                jobworkerp_server_id BIGINT NOT NULL,
                job_id BIGINT DEFAULT NULL,
                triggered_at BIGINT NOT NULL,
                trigger_context_json TEXT DEFAULT NULL,
                enqueue_error TEXT DEFAULT NULL,
                created_at BIGINT NOT NULL,
                result_status INTEGER DEFAULT NULL
            )",
        )
        .execute(&*pool)
        .await
        .unwrap();
        ExecutionRefRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
    }

    #[cfg(feature = "mysql")]
    async fn setup() -> ExecutionRefRepositoryImpl {
        let pool = MYSQL_POOL
            .get_or_init(|| async {
                let pool = infra_utils::infra::rdb::new_rdb_pool(
                    &infra_utils::infra::test::MYSQL_CONFIG,
                    None,
                )
                .await
                .expect("init mysql pool");
                sqlx::raw_sql(sqlx::AssertSqlSafe(MYSQL_SCHEMA.to_string()))
                    .execute(&pool)
                    .await
                    .expect("init mysql schema");
                pool
            })
            .await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "SET FOREIGN_KEY_CHECKS = 0;
             TRUNCATE TABLE `execution_ref`;
             SET FOREIGN_KEY_CHECKS = 1;"
                .to_string(),
        ))
        .execute(pool)
        .await
        .expect("clean execution_ref table");
        ExecutionRefRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
    }

    fn make_ref(source_id: i64, triggered_at: i64, job_id: Option<i64>) -> ExecutionRef {
        ExecutionRef {
            source_type: ExecutionSourceType::CronScheduler as i32,
            source_id,
            source_name: "daily".to_string(),
            jobworkerp_server_id: Some(JobworkerpServerId { value: 10 }),
            job_id,
            triggered_at,
            created_at: triggered_at,
            ..Default::default()
        }
    }

    test_async!(create_and_find_execution_ref, {
        let repo = setup().await;
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo
            .create(&mut *tx, &make_ref(1, 100, Some(200)))
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let found = repo.find(&id).await.unwrap().unwrap();
        assert_eq!(found.source_id, 1);
        assert_eq!(found.job_id, Some(200));
    });

    test_async!(find_latest_by_source_uses_triggered_at, {
        let repo = setup().await;
        for item in [
            make_ref(1, 100, Some(1)),
            make_ref(1, 300, Some(3)),
            make_ref(1, 200, Some(2)),
        ] {
            let mut tx = repo.db_pool().begin().await.unwrap();
            repo.create(&mut *tx, &item).await.unwrap();
            tx.commit().await.unwrap();
        }

        let latest = repo
            .find_latest_by_source(ExecutionSourceType::CronScheduler, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.job_id, Some(3));
    });

    test_async!(create_and_find_preserves_result_status, {
        let repo = setup().await;
        let mut item = make_ref(3, 100, Some(300));
        // FatalError == 2: a terminal failure observed at execution time.
        item.result_status = Some(2);
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo.create(&mut *tx, &item).await.unwrap();
        tx.commit().await.unwrap();

        let found = repo.find(&id).await.unwrap().unwrap();
        assert_eq!(found.result_status, Some(2));
    });

    test_async!(create_without_result_status_keeps_none, {
        let repo = setup().await;
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo
            .create(&mut *tx, &make_ref(4, 100, Some(400)))
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let found = repo.find(&id).await.unwrap().unwrap();
        assert_eq!(found.result_status, None);
    });

    test_async!(update_result_fills_job_id_and_result_status, {
        let repo = setup().await;
        // Pending row recorded at enqueue time: no job_id, no result_status yet.
        let pending = make_ref(5, 100, None);
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo.create(&mut *tx, &pending).await.unwrap();
        tx.commit().await.unwrap();

        let mut tx = repo.db_pool().begin().await.unwrap();
        // FatalError == 2.
        repo.update_result(&mut *tx, &id, Some(500), 2)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let found = repo.find(&id).await.unwrap().unwrap();
        assert_eq!(found.job_id, Some(500));
        assert_eq!(found.result_status, Some(2));
    });

    test_async!(update_enqueue_error_sets_error_without_job_id, {
        let repo = setup().await;
        let pending = make_ref(6, 100, None);
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo.create(&mut *tx, &pending).await.unwrap();
        tx.commit().await.unwrap();

        let mut tx = repo.db_pool().begin().await.unwrap();
        repo.update_enqueue_error(&mut *tx, &id, "connection refused")
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let found = repo.find(&id).await.unwrap().unwrap();
        assert_eq!(found.enqueue_error.as_deref(), Some("connection refused"));
        assert_eq!(found.job_id, None);
        assert_eq!(found.result_status, None);
    });

    // A cancelled row must not be overwritten by a later terminal update: cancelling a PENDING job
    // produces no JobResult, so the concurrently-awaited streaming terminal resolves to a
    // JobResult-less Success that would otherwise clobber the recorded Cancelled.
    test_async!(update_result_does_not_overwrite_cancelled, {
        let repo = setup().await;
        let pending = make_ref(7, 100, None);
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo.create(&mut *tx, &pending).await.unwrap();
        tx.commit().await.unwrap();

        // Cancel records the terminal Cancelled status (6).
        let mut tx = repo.db_pool().begin().await.unwrap();
        repo.update_result(&mut *tx, &id, Some(700), super::RESULT_STATUS_CANCELLED)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // The late streaming terminal resolves to Success (0) and tries to overwrite.
        let mut tx = repo.db_pool().begin().await.unwrap();
        repo.update_result(&mut *tx, &id, Some(700), 0)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let found = repo.find(&id).await.unwrap().unwrap();
        // Cancelled wins regardless of update ordering.
        assert_eq!(found.result_status, Some(super::RESULT_STATUS_CANCELLED));
    });

    // A non-cancelled terminal status is still updatable (the guard only protects Cancelled).
    test_async!(update_result_overwrites_non_cancelled_status, {
        let repo = setup().await;
        let pending = make_ref(8, 100, None);
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo.create(&mut *tx, &pending).await.unwrap();
        tx.commit().await.unwrap();

        // First a transient ErrorAndRetry (1), then the final FatalError (2).
        let mut tx = repo.db_pool().begin().await.unwrap();
        repo.update_result(&mut *tx, &id, Some(800), 1)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let mut tx = repo.db_pool().begin().await.unwrap();
        repo.update_result(&mut *tx, &id, Some(800), 2)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let found = repo.find(&id).await.unwrap().unwrap();
        assert_eq!(found.result_status, Some(2));
    });

    // Updating a missing id is a no-op (best-effort), not an error.
    test_async!(update_result_missing_id_is_ok, {
        let repo = setup().await;
        let mut tx = repo.db_pool().begin().await.unwrap();
        let res = repo
            .update_result(&mut *tx, &ExecutionRefId { value: 999_999 }, Some(1), 0)
            .await;
        tx.commit().await.unwrap();
        assert!(res.is_ok());
    });

    test_async!(create_enqueue_error_without_job_id, {
        let repo = setup().await;
        let mut item = make_ref(2, 100, None);
        item.enqueue_error = Some("unavailable".to_string());
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo.create(&mut *tx, &item).await.unwrap();
        tx.commit().await.unwrap();

        let found = repo.find(&id).await.unwrap().unwrap();
        assert_eq!(found.job_id, None);
        assert_eq!(found.enqueue_error.as_deref(), Some("unavailable"));
    });

    // ---- B3: delete ----

    test_async!(delete_existing_returns_true_missing_returns_false, {
        let repo = setup().await;
        let mut tx = repo.db_pool().begin().await.unwrap();
        let id = repo
            .create(&mut *tx, &make_ref(1, 100, Some(1)))
            .await
            .unwrap();
        tx.commit().await.unwrap();

        assert!(repo.delete(&id).await.unwrap());
        assert!(repo.find(&id).await.unwrap().is_none());
        // Second delete of the same (now missing) id is false.
        assert!(!repo.delete(&id).await.unwrap());
    });

    test_async!(delete_by_ids_removes_only_listed, {
        let repo = setup().await;
        let mut ids = Vec::new();
        for s in 1..=3 {
            let mut tx = repo.db_pool().begin().await.unwrap();
            ids.push(
                repo.create(&mut *tx, &make_ref(s, 100, Some(s)))
                    .await
                    .unwrap(),
            );
            tx.commit().await.unwrap();
        }
        // Delete the first two, keep the third.
        let deleted = repo
            .delete_by_ids(&[ids[0].value, ids[1].value])
            .await
            .unwrap();
        assert_eq!(deleted, 2);
        assert!(repo.find(&ids[0]).await.unwrap().is_none());
        assert!(repo.find(&ids[1]).await.unwrap().is_none());
        assert!(repo.find(&ids[2]).await.unwrap().is_some());
    });

    test_async!(delete_by_ids_empty_is_zero, {
        let repo = setup().await;
        assert_eq!(repo.delete_by_ids(&[]).await.unwrap(), 0);
    });

    // More ids than the chunk size (900) must all be deleted across multiple statements, never
    // failing on the database bind-parameter limit. Uses explicit ids to avoid 1000+ snowflake gens.
    test_async!(delete_by_ids_chunks_beyond_bind_limit, {
        let repo = setup().await;
        // One more than the chunk size forces a second chunk, exercising the bind-limit split.
        let total = DELETE_CHUNK_SIZE as i64 + 1;
        let mut ids = Vec::with_capacity(total as usize);
        for i in 0..total {
            let id = 1_000_000 + i;
            sqlx::query(
                "INSERT INTO execution_ref
                 (id, source_type, source_id, source_name, jobworkerp_server_id, triggered_at, created_at)
                 VALUES (?, 1, 1, 'n', 1, 100, 100)",
            )
            .bind(id)
            .execute(repo.db_pool())
            .await
            .unwrap();
            ids.push(id);
        }
        let deleted = repo.delete_by_ids(&ids).await.unwrap();
        assert_eq!(deleted, total as u64);
        assert_eq!(
            repo.count_list(&ExecutionRefListFilter::default())
                .await
                .unwrap(),
            0
        );
    });

    test_async!(delete_all_by_source_counts_only_that_source, {
        let repo = setup().await;
        // Two refs for source_id=1, one for source_id=2 (same source_type).
        for (sid, t) in [(1, 100), (1, 200), (2, 100)] {
            let mut tx = repo.db_pool().begin().await.unwrap();
            repo.create(&mut *tx, &make_ref(sid, t, None))
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        let deleted = repo
            .delete_all_by_source(ExecutionSourceType::CronScheduler, 1)
            .await
            .unwrap();
        assert_eq!(deleted, 2);
        // source_id=2 survives.
        let remaining = repo
            .find_list_by_source(ExecutionSourceType::CronScheduler, 2, None, None)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
    });

    // ---- B4: find_list / count_list ----

    fn make_ref_full(
        source_type: ExecutionSourceType,
        source_id: i64,
        server_id: i64,
        triggered_at: i64,
    ) -> ExecutionRef {
        ExecutionRef {
            source_type: source_type as i32,
            source_id,
            source_name: "n".to_string(),
            jobworkerp_server_id: Some(JobworkerpServerId { value: server_id }),
            triggered_at,
            created_at: triggered_at,
            ..Default::default()
        }
    }

    async fn seed_for_filters(repo: &ExecutionRefRepositoryImpl) {
        for item in [
            make_ref_full(ExecutionSourceType::CronScheduler, 1, 10, 100),
            make_ref_full(ExecutionSourceType::CronScheduler, 2, 20, 200),
            make_ref_full(ExecutionSourceType::SlackEventHandler, 3, 10, 300),
        ] {
            let mut tx = repo.db_pool().begin().await.unwrap();
            repo.create(&mut *tx, &item).await.unwrap();
            tx.commit().await.unwrap();
        }
    }

    test_async!(find_list_no_filter_is_triggered_at_desc, {
        let repo = setup().await;
        seed_for_filters(&repo).await;
        let list = repo
            .find_list(&ExecutionRefListFilter::default(), None, None)
            .await
            .unwrap();
        let ts: Vec<i64> = list.iter().map(|r| r.triggered_at).collect();
        assert_eq!(ts, vec![300, 200, 100]);
    });

    test_async!(find_list_filters_source_type_and_server_and_window, {
        let repo = setup().await;
        seed_for_filters(&repo).await;

        let by_type = repo
            .find_list(
                &ExecutionRefListFilter {
                    source_type: Some(ExecutionSourceType::CronScheduler as i32),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(by_type.len(), 2);

        let by_server = repo
            .find_list(
                &ExecutionRefListFilter {
                    jobworkerp_server_id: Some(10),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(by_server.len(), 2); // server 10: source 1 and 3

        let windowed = repo
            .find_list(
                &ExecutionRefListFilter {
                    triggered_after: Some(150),
                    triggered_before: Some(250),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(windowed.len(), 1);
        assert_eq!(windowed[0].triggered_at, 200);
    });

    test_async!(find_list_limit_offset_paging, {
        let repo = setup().await;
        seed_for_filters(&repo).await;
        let page1 = repo
            .find_list(&ExecutionRefListFilter::default(), Some(2), Some(0))
            .await
            .unwrap();
        assert_eq!(
            page1.iter().map(|r| r.triggered_at).collect::<Vec<_>>(),
            vec![300, 200]
        );
        let page2 = repo
            .find_list(&ExecutionRefListFilter::default(), Some(2), Some(2))
            .await
            .unwrap();
        assert_eq!(
            page2.iter().map(|r| r.triggered_at).collect::<Vec<_>>(),
            vec![100]
        );
    });

    test_async!(count_list_matches_filter, {
        let repo = setup().await;
        seed_for_filters(&repo).await;
        assert_eq!(
            repo.count_list(&ExecutionRefListFilter::default())
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            repo.count_list(&ExecutionRefListFilter {
                source_type: Some(ExecutionSourceType::CronScheduler as i32),
                ..Default::default()
            })
            .await
            .unwrap(),
            2
        );
    });
}
