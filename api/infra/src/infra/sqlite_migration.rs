//! Startup schema migration for SQLite deployments.
//!
//! Databases created before migrations were tracked hold an older
//! `002_schema.sql` whose tables lack later columns, and `CREATE TABLE IF NOT
//! EXISTS` never adds them. Such databases are first brought up to the
//! baseline by adding the missing columns, then sqlx applies the remaining
//! versioned migrations.

use anyhow::{bail, Context, Result};
use infra_utils::infra::rdb::{new_rdb_pool, RdbConfig};
use sqlx::{migrate::Migrator, Connection, Row, SqliteConnection, SqlitePool};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

static MIGRATOR: Migrator = sqlx::migrate!("sql/sqlite");

/// The migration whose schema every pre-tracking database is upgraded to.
/// Its file must never change: applied databases record its checksum.
const BASELINE_VERSION: i64 = 2;
const BASELINE_SCHEMA: &str = include_str!("../../sql/sqlite/002_schema.sql");
const MIGRATIONS_TABLE: &str = "_sqlx_migrations";
const BACKUP_SUFFIX: &str = ".pre-migration.bak";

/// What a migration run changed.
#[derive(Debug, Default)]
struct MigrationReport {
    /// Whether any migration was applied.
    schema_changed: bool,
    /// Copy of the database taken before any change, when one was needed.
    backup_path: Option<PathBuf>,
    /// `table.column` names added to reach the baseline schema.
    added_columns: Vec<String>,
}

/// Open the application pool on a migrated database.
pub async fn open_migrated_pool(config: &RdbConfig) -> Result<SqlitePool> {
    let pool = new_rdb_pool(config, None).await?;
    migrate_then_reopen(pool, config).await
}

/// A connection that loaded the schema before a migration keeps it, and sqlx
/// then reads `SELECT *` rows with the old column count and crashes its
/// worker. So after a change the migrating pool is discarded and the
/// application gets fresh connections.
async fn migrate_then_reopen(pool: SqlitePool, config: &RdbConfig) -> Result<SqlitePool> {
    let migrated = migrate(&pool).await;
    if matches!(
        migrated,
        Ok(MigrationReport {
            schema_changed: false,
            ..
        })
    ) {
        return Ok(pool);
    }
    pool.close().await;
    migrated?;
    new_rdb_pool(config, None).await
}

async fn migrate(pool: &SqlitePool) -> Result<MigrationReport> {
    let tables = user_tables(pool).await?;
    let applied = applied_versions(pool, &tables).await?;
    let has_data = tables.iter().any(|t| t != MIGRATIONS_TABLE);
    let mut report = MigrationReport {
        schema_changed: MIGRATOR
            .iter()
            .any(|m| !m.migration_type.is_down_migration() && !applied.contains(&m.version)),
        ..Default::default()
    };

    if report.schema_changed && has_data {
        report.backup_path = backup(pool).await.context("back up before migration")?;
    }
    let with_backup = |e: anyhow::Error| match &report.backup_path {
        Some(path) => e.context(format!("pre-migration backup: {}", path.display())),
        None => e,
    };

    if has_data && !applied.contains(&BASELINE_VERSION) {
        report.added_columns = add_missing_baseline_columns(pool)
            .await
            .context("upgrade untracked database to the baseline schema")
            .map_err(with_backup)?;
    }
    // Also run when nothing is pending: it rejects edited or unknown applied
    // migrations by checksum and version.
    MIGRATOR
        .run(pool)
        .await
        .context("apply sqlite migrations")
        .map_err(with_backup)?;
    Ok(report)
}

/// Versions recorded as successfully applied. A failed record is fatal:
/// the schema is in an unknown state and needs manual recovery.
async fn applied_versions(pool: &SqlitePool, tables: &BTreeSet<String>) -> Result<BTreeSet<i64>> {
    if !tables.contains(MIGRATIONS_TABLE) {
        return Ok(BTreeSet::new());
    }
    let rows = sqlx::query("SELECT version, success FROM _sqlx_migrations")
        .fetch_all(pool)
        .await?;
    let mut applied = BTreeSet::new();
    for row in rows {
        let version: i64 = row.get(0);
        if !row.get::<bool, _>(1) {
            bail!("migration {version} is recorded as failed; restore a backup taken before it");
        }
        applied.insert(version);
    }
    Ok(applied)
}

async fn user_tables<'c, E>(executor: E) -> Result<BTreeSet<String>>
where
    E: sqlx::SqliteExecutor<'c>,
{
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(executor)
    .await?;
    Ok(names.into_iter().collect())
}

/// Write a consistent copy next to the database file. Returns `None` for
/// in-memory databases, which have nothing to preserve across restarts.
async fn backup(pool: &SqlitePool) -> Result<Option<PathBuf>> {
    let file: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(pool)
            .await?;
    if file.is_empty() {
        return Ok(None);
    }
    let backup = PathBuf::from(format!("{file}{BACKUP_SUFFIX}"));
    // VACUUM INTO refuses to overwrite, and writing beside the final name
    // keeps the previous backup intact until the new one is complete.
    let partial = PathBuf::from(format!("{file}{BACKUP_SUFFIX}.partial"));
    remove_if_exists(&partial)?;
    create_with_permissions_of(&partial, std::path::Path::new(&file))?;
    sqlx::query("VACUUM INTO ?")
        .bind(partial.to_string_lossy().into_owned())
        .execute(pool)
        .await?;
    std::fs::rename(&partial, &backup)?;
    tracing::info!("backed up sqlite database to {}", backup.display());
    Ok(Some(backup))
}

/// Create `path` empty with `source`'s permissions. VACUUM INTO accepts an
/// empty target, and creating it first keeps the copy from ever being more
/// readable than the database (SQLite would create it from the umask).
fn create_with_permissions_of(path: &std::path::Path, source: &std::path::Path) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mode = std::fs::metadata(source)?.permissions().mode() & 0o777;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = source;
    options
        .open(path)
        .with_context(|| format!("create backup file {}", path.display()))?;
    Ok(())
}

fn remove_if_exists(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

#[derive(Debug, PartialEq)]
struct ColumnDef {
    name: String,
    decl_type: String,
    not_null: bool,
    default: Option<String>,
    primary_key: bool,
}

impl ColumnDef {
    fn add_column_sql(&self, table: &str) -> Result<String> {
        // SQLite rejects ADD COLUMN for these; the table must be rebuilt by hand.
        if self.primary_key || (self.not_null && self.default.is_none()) {
            bail!(
                "cannot add required column {table}.{} to an existing table",
                self.name
            );
        }
        let mut sql = format!("ALTER TABLE `{table}` ADD COLUMN `{}`", self.name);
        if !self.decl_type.is_empty() {
            sql.push(' ');
            sql.push_str(&self.decl_type);
        }
        if self.not_null {
            sql.push_str(" NOT NULL");
        }
        if let Some(default) = &self.default {
            sql.push_str(" DEFAULT ");
            sql.push_str(default);
        }
        Ok(sql)
    }
}

async fn columns<'c, E>(executor: E, table: &str) -> Result<Vec<ColumnDef>>
where
    E: sqlx::SqliteExecutor<'c>,
{
    let rows =
        sqlx::query("SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?)")
            .bind(table)
            .fetch_all(executor)
            .await?;
    Ok(rows
        .iter()
        .map(|r| ColumnDef {
            name: r.get(0),
            decl_type: r.get(1),
            not_null: r.get(2),
            default: r.get(3),
            primary_key: r.get::<i64, _>(4) > 0,
        })
        .collect())
}

/// Column definitions of the baseline schema, read from a scratch database
/// so the schema file stays the single source of truth.
async fn baseline_columns() -> Result<BTreeMap<String, Vec<ColumnDef>>> {
    let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
    sqlx::raw_sql(BASELINE_SCHEMA).execute(&mut conn).await?;
    let mut baseline = BTreeMap::new();
    for table in user_tables(&mut conn).await? {
        let defs = columns(&mut conn, &table).await?;
        baseline.insert(table, defs);
    }
    conn.close().await?;
    Ok(baseline)
}

/// Add every baseline column missing from an existing table in one
/// transaction. Missing tables and indexes are left to the baseline
/// migration, whose statements are all `IF NOT EXISTS`.
async fn add_missing_baseline_columns(pool: &SqlitePool) -> Result<Vec<String>> {
    let baseline = baseline_columns().await?;
    let mut tx = pool.begin().await?;
    let existing_tables = user_tables(&mut *tx).await?;
    let mut added = Vec::new();
    for (table, defs) in &baseline {
        if !existing_tables.contains(table) {
            continue;
        }
        let existing: BTreeSet<String> = columns(&mut *tx, table)
            .await?
            .into_iter()
            .map(|c| c.name)
            .collect();
        for def in defs.iter().filter(|d| !existing.contains(&d.name)) {
            let sql = def.add_column_sql(table)?;
            sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(&mut *tx)
                .await?;
            added.push(format!("{table}.{}", def.name));
        }
    }
    tx.commit().await?;
    if !added.is_empty() {
        tracing::info!("added baseline columns: {}", added.join(", "));
    }
    Ok(added)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SQLite file in its own temp dir, removed with its side files.
    struct TestDb {
        _dir: tempfile::TempDir,
        path: PathBuf,
    }

    impl TestDb {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("conductor.sqlite3");
            Self { _dir: dir, path }
        }

        fn config(&self, max_connections: u32) -> RdbConfig {
            let url = format!("sqlite://{}?mode=rwc", self.path.display());
            RdbConfig::new_by_url(&url, max_connections)
        }

        async fn pool(&self) -> SqlitePool {
            new_rdb_pool(&self.config(2), None).await.unwrap()
        }

        fn backup_path(&self) -> PathBuf {
            PathBuf::from(format!("{}{BACKUP_SUFFIX}", self.path.display()))
        }
    }

    /// Schema as written before the worker execution mode, `job_timeout_sec`
    /// and `execution_ref` changes, including the oldest `cron_scheduler`
    /// without `args`.
    const LEGACY_SCHEMA: &str = r#"
        CREATE TABLE `jobworkerp_server` (
            `id` BIGINT NOT NULL PRIMARY KEY, `name` TEXT NOT NULL, `host` TEXT NOT NULL,
            `port` TEXT NOT NULL, `ssl_enabled` TINYINT UNSIGNED NOT NULL, `description` TEXT,
            `enabled` TINYINT UNSIGNED NOT NULL, `created_at` BIGINT, `updated_at` BIGINT
        );
        CREATE TABLE `cron_scheduler` (
            `id` BIGINT NOT NULL PRIMARY KEY, `name` TEXT NOT NULL,
            `jobworkerp_server_id` BIGINT NOT NULL, `workflow_url` TEXT NOT NULL, `channel` TEXT,
            `crontab` TEXT NOT NULL, `enabled` TINYINT UNSIGNED NOT NULL, `description` TEXT,
            `created_at` BIGINT, `updated_at` BIGINT
        );
        CREATE TABLE `worker_result_handler` (
            `id` BIGINT NOT NULL PRIMARY KEY, `name` TEXT NOT NULL,
            `listen_jobworkerp_server_id` BIGINT NOT NULL, `listen_worker_name` TEXT NOT NULL,
            `process_jobworkerp_server_id` BIGINT NOT NULL, `workflow_url` TEXT NOT NULL,
            `channel` TEXT, `enabled` TINYINT UNSIGNED NOT NULL, `description` TEXT,
            `args` TEXT DEFAULT NULL, `created_at` BIGINT, `updated_at` BIGINT
        );
        CREATE TABLE `slack_event_handler` (
            `id` BIGINT NOT NULL PRIMARY KEY, `name` TEXT NOT NULL, `description` TEXT,
            `enabled` TINYINT UNSIGNED NOT NULL DEFAULT 1, `slack_channel_id` TEXT,
            `message_pattern` TEXT, `mention_required` TINYINT UNSIGNED DEFAULT 0,
            `reaction_names` TEXT, `reaction_operation` TEXT, `reaction_user_filter` TEXT,
            `jobworkerp_server_id` BIGINT NOT NULL, `workflow_url` TEXT NOT NULL, `channel` TEXT,
            `timeout_sec` INTEGER NOT NULL DEFAULT 3600, `args` TEXT,
            `created_at` BIGINT NOT NULL, `updated_at` BIGINT NOT NULL
        );
        INSERT INTO `jobworkerp_server` VALUES (1, 'local', '127.0.0.1', '9000', 0, NULL, 1, 10, 10);
        INSERT INTO `cron_scheduler` VALUES (11, 'nightly', 1, 'file:///w.yaml', NULL, '0 0 * * * *', 1, 'keep me', 20, 21);
    "#;

    async fn exec(pool: &SqlitePool, sql: &'static str) {
        sqlx::raw_sql(sql).execute(pool).await.unwrap();
    }

    /// Column definitions per table, keyed by name because ALTER appends
    /// columns at the end.
    async fn schema_shape(pool: &SqlitePool) -> BTreeMap<String, BTreeMap<String, ColumnDef>> {
        let mut shape = BTreeMap::new();
        for table in user_tables(pool).await.unwrap() {
            if table == MIGRATIONS_TABLE {
                continue;
            }
            let defs = columns(pool, &table).await.unwrap();
            shape.insert(
                table,
                defs.into_iter().map(|d| (d.name.clone(), d)).collect(),
            );
        }
        shape
    }

    async fn index_names(pool: &SqlitePool) -> BTreeSet<String> {
        sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .collect()
    }

    async fn recorded_versions(pool: &SqlitePool) -> Vec<i64> {
        let tables = user_tables(pool).await.unwrap();
        let applied = applied_versions(pool, &tables).await.unwrap();
        applied.into_iter().collect()
    }

    async fn column_names(pool: &SqlitePool, table: &str) -> BTreeSet<String> {
        let defs = columns(pool, table).await.unwrap();
        defs.into_iter().map(|d| d.name).collect()
    }

    fn all_versions() -> Vec<i64> {
        MIGRATOR.iter().map(|m| m.version).collect()
    }

    #[tokio::test]
    async fn fresh_database_gets_every_migration_without_backup() {
        let db = TestDb::new();
        let pool = db.pool().await;

        let report = migrate(&pool).await.unwrap();

        assert_eq!(recorded_versions(&pool).await, all_versions());
        assert!(column_names(&pool, "cron_scheduler")
            .await
            .contains("job_timeout_sec"));
        assert!(column_names(&pool, "execution_ref")
            .await
            .contains("result_status"));
        assert!(report.added_columns.is_empty());
        assert_eq!(report.backup_path, None);
        assert!(!db.backup_path().exists());
    }

    #[tokio::test]
    async fn legacy_database_is_upgraded_to_the_fresh_schema_and_keeps_rows() {
        let fresh_db = TestDb::new();
        let fresh = fresh_db.pool().await;
        migrate(&fresh).await.unwrap();

        let db = TestDb::new();
        let pool = db.pool().await;
        exec(&pool, LEGACY_SCHEMA).await;

        let report = migrate(&pool).await.unwrap();

        assert!(report.schema_changed);
        assert_eq!(schema_shape(&pool).await, schema_shape(&fresh).await);
        assert_eq!(index_names(&pool).await, index_names(&fresh).await);
        assert_eq!(recorded_versions(&pool).await, all_versions());
        let mut added = report.added_columns;
        added.sort();
        assert_eq!(
            added,
            [
                "cron_scheduler.args",
                "cron_scheduler.job_timeout_sec",
                "cron_scheduler.using",
                "cron_scheduler.worker_name",
                "slack_event_handler.using",
                "slack_event_handler.worker_name",
                "worker_result_handler.using",
                "worker_result_handler.worker_name",
            ]
        );

        let row = sqlx::query(
            "SELECT name, description, created_at, job_timeout_sec, worker_name \
             FROM cron_scheduler WHERE id = 11",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>(0), "nightly");
        assert_eq!(row.get::<String, _>(1), "keep me");
        assert_eq!(row.get::<i64, _>(2), 20);
        assert_eq!(row.get::<Option<i64>, _>(3), None);
        assert_eq!(row.get::<Option<String>, _>(4), None);

        let backup = report
            .backup_path
            .expect("legacy upgrade must back up first");
        assert_eq!(backup, db.backup_path());
        let backup_pool = SqlitePool::connect(&format!("sqlite://{}", backup.display()))
            .await
            .unwrap();
        let backup_columns = column_names(&backup_pool, "cron_scheduler").await;
        assert!(!backup_columns.contains("job_timeout_sec"));
        let backed_up: String =
            sqlx::query_scalar("SELECT description FROM cron_scheduler WHERE id = 11")
                .fetch_one(&backup_pool)
                .await
                .unwrap();
        assert_eq!(backed_up, "keep me");
    }

    #[tokio::test]
    async fn complete_but_untracked_database_only_records_the_baseline() {
        let db = TestDb::new();
        let pool = db.pool().await;
        exec(&pool, BASELINE_SCHEMA).await;

        let report = migrate(&pool).await.unwrap();

        assert!(report.added_columns.is_empty());
        assert_eq!(recorded_versions(&pool).await, all_versions());
    }

    #[tokio::test]
    async fn rerun_on_migrated_database_is_a_no_op() {
        let db = TestDb::new();
        let pool = db.pool().await;
        exec(&pool, LEGACY_SCHEMA).await;
        migrate(&pool).await.unwrap();
        std::fs::remove_file(db.backup_path()).unwrap();

        let report = migrate(&pool).await.unwrap();

        assert!(!report.schema_changed);
        assert!(report.added_columns.is_empty());
        assert_eq!(report.backup_path, None);
        assert!(!db.backup_path().exists());
        assert_eq!(recorded_versions(&pool).await, all_versions());
    }

    #[tokio::test]
    async fn unaddable_missing_column_fails_without_partial_changes() {
        let db = TestDb::new();
        let pool = db.pool().await;
        exec(&pool, LEGACY_SCHEMA).await;
        // `host` is NOT NULL without a default, so ALTER TABLE cannot add it.
        exec(&pool, "ALTER TABLE `jobworkerp_server` DROP COLUMN `host`").await;

        let error = migrate(&pool).await.unwrap_err();

        assert!(
            format!("{error:#}").contains("jobworkerp_server.host"),
            "{error:#}"
        );
        assert!(!column_names(&pool, "cron_scheduler")
            .await
            .contains("job_timeout_sec"));
    }

    #[tokio::test]
    async fn failed_migration_record_stops_startup() {
        let db = TestDb::new();
        let pool = db.pool().await;
        migrate(&pool).await.unwrap();
        exec(
            &pool,
            "UPDATE _sqlx_migrations SET success = 0 WHERE version = 2",
        )
        .await;

        assert!(migrate(&pool).await.is_err());
    }

    #[tokio::test]
    async fn connections_that_saw_the_old_schema_are_not_reused() {
        const CONNECTIONS: u32 = 5;
        let db = TestDb::new();
        let config = db.config(CONNECTIONS);
        let pool = new_rdb_pool(&config, None).await.unwrap();
        exec(&pool, LEGACY_SCHEMA).await;
        // Load the pre-migration schema into every pooled connection, as the
        // pool's own setup statements can do on an existing database.
        let mut held = Vec::new();
        for _ in 0..CONNECTIONS {
            let mut conn = pool.acquire().await.unwrap();
            sqlx::query("SELECT count(*) FROM cron_scheduler")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            held.push(conn);
        }
        drop(held);

        let pool = migrate_then_reopen(pool, &config).await.unwrap();

        let mut held = Vec::new();
        for _ in 0..CONNECTIONS {
            held.push(pool.acquire().await.unwrap());
        }
        for conn in &mut held {
            let row = sqlx::query("SELECT * FROM cron_scheduler WHERE id = 11")
                .fetch_one(&mut **conn)
                .await
                .unwrap();
            assert_eq!(row.get::<Option<i64>, _>("job_timeout_sec"), None);
            assert_eq!(row.get::<String, _>("name"), "nightly");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn backup_keeps_the_database_file_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let db = TestDb::new();
        let pool = db.pool().await;
        exec(&pool, LEGACY_SCHEMA).await;
        // 0o600 would widen to 0o644 under the usual umask 022 if the backup
        // were created with default permissions.
        std::fs::set_permissions(&db.path, std::fs::Permissions::from_mode(0o600)).unwrap();

        let report = migrate(&pool).await.unwrap();

        let backup = report.backup_path.unwrap();
        let mode = std::fs::metadata(&backup).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "backup mode {mode:o}");
    }
}
