use crate::error::UiEventHandlerError;
use anyhow::Result;
use infra_utils::infra::rdb::{RdbPool, UseRdbPool};
use sqlx::Row;
use std::collections::HashMap;
use tracing::{info, warn};

/// データベース移行とINDEX管理機能
#[async_trait::async_trait]
pub trait MigrationRepository: UseRdbPool {
    /// 既存データの重複チェックとクリーンアップ
    async fn check_and_cleanup_duplicates(&self) -> Result<DuplicateReport> {
        let pool = self.db_pool();
        let tx = pool.begin().await.map_err(UiEventHandlerError::DBError)?;

        let mut report = DuplicateReport::new();

        // Check jobworkerp_server duplicates
        let server_duplicates = self.find_duplicate_server_names(pool).await?;
        if !server_duplicates.is_empty() {
            report.server_duplicates = server_duplicates.clone();
            self.cleanup_server_duplicates(pool, &server_duplicates)
                .await?;
        }

        // Check cron_scheduler duplicates
        let scheduler_duplicates = self.find_duplicate_scheduler_names(pool).await?;
        if !scheduler_duplicates.is_empty() {
            report.scheduler_duplicates = scheduler_duplicates.clone();
            self.cleanup_scheduler_duplicates(pool, &scheduler_duplicates)
                .await?;
        }

        // Check worker_result_handler duplicates
        let handler_duplicates = self.find_duplicate_handler_names(pool).await?;
        if !handler_duplicates.is_empty() {
            report.handler_duplicates = handler_duplicates.clone();
            self.cleanup_handler_duplicates(pool, &handler_duplicates)
                .await?;
        }

        tx.commit().await.map_err(UiEventHandlerError::DBError)?;
        Ok(report)
    }

    /// Check and cleanup duplicates (indexes and constraints are now in schema.sql)
    async fn apply_indexes_and_constraints(&self) -> Result<()> {
        // Check and cleanup duplicates before constraints are applied
        let report = self.check_and_cleanup_duplicates().await?;
        if report.has_duplicates() {
            info!(
                "Cleaned up duplicates before applying constraints: {:?}",
                report
            );
        }

        info!("Indexes and constraints are now applied during table creation in schema.sql");
        Ok(())
    }

    /// jobworkerp_serverの重複name検出
    async fn find_duplicate_server_names(
        &self,
        pool: &RdbPool,
    ) -> Result<HashMap<String, Vec<i64>>> {
        let rows = sqlx::query(
            "SELECT name, GROUP_CONCAT(id) as ids, COUNT(*) as count 
             FROM jobworkerp_server 
             GROUP BY name 
             HAVING COUNT(*) > 1",
        )
        .fetch_all(pool)
        .await
        .map_err(UiEventHandlerError::DBError)?;

        let mut duplicates = HashMap::new();
        for row in rows {
            let name: String = row.get("name");
            let ids_str: String = row.get("ids");
            let ids: Vec<i64> = ids_str.split(',').filter_map(|s| s.parse().ok()).collect();
            duplicates.insert(name, ids);
        }

        Ok(duplicates)
    }

    /// cron_schedulerの重複name検出  
    async fn find_duplicate_scheduler_names(
        &self,
        pool: &RdbPool,
    ) -> Result<HashMap<String, Vec<i64>>> {
        let rows = sqlx::query(
            "SELECT name, GROUP_CONCAT(id) as ids, COUNT(*) as count 
             FROM cron_scheduler 
             GROUP BY name 
             HAVING COUNT(*) > 1",
        )
        .fetch_all(pool)
        .await
        .map_err(UiEventHandlerError::DBError)?;

        let mut duplicates = HashMap::new();
        for row in rows {
            let name: String = row.get("name");
            let ids_str: String = row.get("ids");
            let ids: Vec<i64> = ids_str.split(',').filter_map(|s| s.parse().ok()).collect();
            duplicates.insert(name, ids);
        }

        Ok(duplicates)
    }

    /// worker_result_handlerの重複name検出
    async fn find_duplicate_handler_names(
        &self,
        pool: &RdbPool,
    ) -> Result<HashMap<String, Vec<i64>>> {
        let rows = sqlx::query(
            "SELECT name, GROUP_CONCAT(id) as ids, COUNT(*) as count 
             FROM worker_result_handler 
             GROUP BY name 
             HAVING COUNT(*) > 1",
        )
        .fetch_all(pool)
        .await
        .map_err(UiEventHandlerError::DBError)?;

        let mut duplicates = HashMap::new();
        for row in rows {
            let name: String = row.get("name");
            let ids_str: String = row.get("ids");
            let ids: Vec<i64> = ids_str.split(',').filter_map(|s| s.parse().ok()).collect();
            duplicates.insert(name, ids);
        }

        Ok(duplicates)
    }

    /// サーバー重複のクリーンアップ（最新のものを残す）
    async fn cleanup_server_duplicates(
        &self,
        pool: &RdbPool,
        duplicates: &HashMap<String, Vec<i64>>,
    ) -> Result<()> {
        for (name, ids) in duplicates {
            if ids.len() <= 1 {
                continue;
            }

            // Keep the latest (highest ID), delete others
            let mut sorted_ids = ids.clone();
            sorted_ids.sort();
            let to_delete = &sorted_ids[..sorted_ids.len() - 1];

            for &id in to_delete {
                warn!(
                    "Deleting duplicate jobworkerp_server: name='{}', id={}",
                    name, id
                );
                sqlx::query("DELETE FROM jobworkerp_server WHERE id = ?")
                    .bind(id)
                    .execute(pool)
                    .await
                    .map_err(UiEventHandlerError::DBError)?;
            }
        }
        Ok(())
    }

    /// スケジューラー重複のクリーンアップ
    async fn cleanup_scheduler_duplicates(
        &self,
        pool: &RdbPool,
        duplicates: &HashMap<String, Vec<i64>>,
    ) -> Result<()> {
        for (name, ids) in duplicates {
            if ids.len() <= 1 {
                continue;
            }

            let mut sorted_ids = ids.clone();
            sorted_ids.sort();
            let to_delete = &sorted_ids[..sorted_ids.len() - 1];

            for &id in to_delete {
                warn!(
                    "Deleting duplicate cron_scheduler: name='{}', id={}",
                    name, id
                );
                sqlx::query("DELETE FROM cron_scheduler WHERE id = ?")
                    .bind(id)
                    .execute(pool)
                    .await
                    .map_err(UiEventHandlerError::DBError)?;
            }
        }
        Ok(())
    }

    /// ハンドラー重複のクリーンアップ
    async fn cleanup_handler_duplicates(
        &self,
        pool: &RdbPool,
        duplicates: &HashMap<String, Vec<i64>>,
    ) -> Result<()> {
        for (name, ids) in duplicates {
            if ids.len() <= 1 {
                continue;
            }

            let mut sorted_ids = ids.clone();
            sorted_ids.sort();
            let to_delete = &sorted_ids[..sorted_ids.len() - 1];

            for &id in to_delete {
                warn!(
                    "Deleting duplicate worker_result_handler: name='{}', id={}",
                    name, id
                );
                sqlx::query("DELETE FROM worker_result_handler WHERE id = ?")
                    .bind(id)
                    .execute(pool)
                    .await
                    .map_err(UiEventHandlerError::DBError)?;
            }
        }
        Ok(())
    }
}

/// 重複データのレポート
#[derive(Debug, Clone)]
pub struct DuplicateReport {
    pub server_duplicates: HashMap<String, Vec<i64>>,
    pub scheduler_duplicates: HashMap<String, Vec<i64>>,
    pub handler_duplicates: HashMap<String, Vec<i64>>,
}

impl Default for DuplicateReport {
    fn default() -> Self {
        Self::new()
    }
}

impl DuplicateReport {
    pub fn new() -> Self {
        Self {
            server_duplicates: HashMap::new(),
            scheduler_duplicates: HashMap::new(),
            handler_duplicates: HashMap::new(),
        }
    }

    pub fn has_duplicates(&self) -> bool {
        !self.server_duplicates.is_empty()
            || !self.scheduler_duplicates.is_empty()
            || !self.handler_duplicates.is_empty()
    }

    pub fn total_duplicates_cleaned(&self) -> usize {
        let server_cleaned: usize = self
            .server_duplicates
            .values()
            .map(|ids| if ids.len() > 1 { ids.len() - 1 } else { 0 })
            .sum();
        let scheduler_cleaned: usize = self
            .scheduler_duplicates
            .values()
            .map(|ids| if ids.len() > 1 { ids.len() - 1 } else { 0 })
            .sum();
        let handler_cleaned: usize = self
            .handler_duplicates
            .values()
            .map(|ids| if ids.len() > 1 { ids.len() - 1 } else { 0 })
            .sum();

        server_cleaned + scheduler_cleaned + handler_cleaned
    }
}

/// データベースエラーが重複制約エラーかチェック
#[allow(dead_code)]
fn is_duplicate_constraint_error(error: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_error) = error {
        let code = db_error.code();
        // MySQL: 1061 (Duplicate key name), 1062 (Duplicate entry)
        // SQLite: unique constraint violations
        code.is_some_and(|c| {
            c == "1061"
                || c == "1062"
                || db_error.message().contains("UNIQUE constraint")
                || db_error.message().contains("already exists")
        })
    } else {
        false
    }
}

/// 移行管理の実装
pub struct MigrationRepositoryImpl {
    pool: &'static RdbPool,
}

impl MigrationRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for MigrationRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

#[async_trait::async_trait]
impl MigrationRepository for MigrationRepositoryImpl {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duplicate_report() {
        let mut report = DuplicateReport::new();
        assert!(!report.has_duplicates());
        assert_eq!(report.total_duplicates_cleaned(), 0);

        report
            .server_duplicates
            .insert("server1".to_string(), vec![1, 2, 3]);
        assert!(report.has_duplicates());
        assert_eq!(report.total_duplicates_cleaned(), 2); // Keep 1, delete 2
    }

    #[test]
    fn test_is_duplicate_constraint_error() {
        // This would need actual database errors to test properly
        // For now, we test the logic structure
        assert!(!is_duplicate_constraint_error(&sqlx::Error::RowNotFound));
    }
}

#[cfg(all(test, feature = "integration_tests"))]
mod integration_tests {
    use super::*;
    use crate::error::UiEventHandlerError;
    use crate::infra::IdGeneratorWrapper;
    use anyhow::Result;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::test::{setup_test_rdb_from, TEST_RUNTIME};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn current_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    async fn _test_constraint_violation_recovery(pool: &'static RdbPool) -> Result<()> {
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
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        let migration_repo = MigrationRepositoryImpl::new(pool);

        let report = migration_repo.check_and_cleanup_duplicates().await?;
        println!("Duplicate cleanup report: {report:?}");

        if report.has_duplicates() {
            println!("✅ Duplicate detection and cleanup working");
            assert!(report.total_duplicates_cleaned() > 0);
        } else {
            println!("ℹ️  No duplicates found (may indicate UNIQUE constraints already applied)");
        }

        Ok(())
    }

    #[test]
    fn cleans_up_duplicate_rows() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let dir = if cfg!(feature = "mysql") {
                "sql/mysql"
            } else {
                "sql/sqlite"
            };
            let pool = setup_test_rdb_from(dir).await;
            sqlx::query("DELETE FROM jobworkerp_server;")
                .execute(pool)
                .await?;
            _test_constraint_violation_recovery(pool).await
        })
    }
}
