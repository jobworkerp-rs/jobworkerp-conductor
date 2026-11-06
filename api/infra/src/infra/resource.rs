use crate::error::UiEventHandlerError;
use anyhow::Result;
use infra_utils::infra::{
    rdb::{Rdb, RdbConfig, RdbUrlConfigImpl},
    redis::{RedisConfig, RedisPool},
};
use sqlx::Pool;

static RDB_POOL: tokio::sync::OnceCell<Pool<Rdb>> = tokio::sync::OnceCell::const_new();

pub async fn setup_rdb_by_env() -> &'static Pool<Rdb> {
    let conf = load_db_config_from_env()
        .unwrap_or(load_db_url_config_from_env().expect("Error on loading rdb config"));
    setup_rdb(&conf).await
}

// new rdb pool and store as static
// (if failed initializing, panic!)
// (if need multiple database, add RDB_POOL and setup multiple)
pub async fn setup_rdb(db_config: &RdbConfig) -> &'static Pool<Rdb> {
    sqlx::any::install_default_drivers();
    RDB_POOL.get_or_init(|| open_pool(db_config)).await
}

#[cfg(not(feature = "mysql"))]
async fn open_pool(db_config: &RdbConfig) -> Pool<Rdb> {
    super::sqlite_migration::open_migrated_pool(db_config)
        .await
        .unwrap_or_else(|e| panic!("sqlite schema migration failed: {e:#}"))
}

// MySQL schemas are migrated by operators; see sql/migration/README.md.
#[cfg(feature = "mysql")]
async fn open_pool(db_config: &RdbConfig) -> Pool<Rdb> {
    infra_utils::infra::rdb::new_rdb_pool(db_config, None)
        .await
        .unwrap()
}

pub fn load_db_url_config_from_env() -> Result<RdbConfig> {
    // sqlite first
    envy::prefixed("SQLITE_")
        .from_env::<RdbUrlConfigImpl>()
        .map(RdbConfig::Url)
        .or_else(|_| {
            envy::prefixed("MYSQL_")
                .from_env::<RdbUrlConfigImpl>()
                .map(RdbConfig::Url)
        })
        .map_err(|e| {
            UiEventHandlerError::RuntimeError(format!("cannot read rdb url config from env: {e:?}"))
                .into()
        })
}

pub fn load_db_config_from_env() -> Option<RdbConfig> {
    // sqlite の設定優先
    envy::prefixed("SQLITE_")
        .from_env::<RdbConfig>()
        .or_else(|_| envy::prefixed("MYSQL_").from_env::<RdbConfig>())
        .ok()
}

// TODO
static _REDIS: tokio::sync::OnceCell<RedisPool> = tokio::sync::OnceCell::const_new();

pub async fn _setup_redis_pool(config: RedisConfig) -> &'static RedisPool {
    _REDIS
        .get_or_init(|| async {
            infra_utils::infra::redis::new_redis_pool(config)
                .await
                .expect("msg")
        })
        .await
}
pub fn _load_redis_config_from_env() -> Result<RedisConfig> {
    envy::prefixed("REDIS_")
        .from_env::<RedisConfig>()
        .map_err(|e| {
            UiEventHandlerError::RuntimeError(format!("cannot read redis config from env: {e:?}"))
                .into()
        })
}
