use anyhow::Result;
use async_trait::async_trait;
use infra::error::UiEventHandlerError;
use infra::infra::cron_scheduler::rdb::{
    CronSchedulerRepository, CronSchedulerRepositoryImpl, UseCronSchedulerRepository,
};
use infra_utils::infra::rdb::UseRdbPool;
use memory_utils::cache::stretto::UseMemoryCache;
use memory_utils::lock::RwLockWithKey;
use proto::jobworkerp_conductor::data::{CronScheduler, CronSchedulerData, CronSchedulerId};
use shared::notification::service::ConfigChangeNotificationService;
use std::{sync::Arc, time::Duration};
use stretto::AsyncCache;

#[async_trait]
pub trait CronSchedulerApp:
    UseCronSchedulerRepository
    + UseMemoryCache<Arc<String>, CronScheduler>
    + Send
    + Sync
    + Sized
    + 'static
{
    fn notification_service(&self) -> &Arc<dyn ConfigChangeNotificationService>;
    fn validate_execution_target(data: &CronSchedulerData) -> Result<()>;
    async fn create_cron_scheduler(
        &self,
        cron_scheduler: &CronSchedulerData,
    ) -> Result<CronSchedulerId>;
    async fn update_cron_scheduler(
        &self,
        id: &CronSchedulerId,
        cron_scheduler: &Option<CronSchedulerData>,
    ) -> Result<bool>;
    async fn delete_cron_scheduler(&self, id: &CronSchedulerId) -> Result<bool>;
    fn find_cache_key(id: &i64) -> String;
    fn find_by_name_cache_key(name: &str) -> String;
    async fn find_cron_scheduler(
        &self,
        id: &CronSchedulerId,
        ttl: Option<&Duration>,
    ) -> Result<Option<CronScheduler>>
    where
        Self: Send + 'static;
    async fn find_cron_scheduler_by_name(
        &self,
        name: &str,
        ttl: Option<&Duration>,
    ) -> Result<Option<CronScheduler>>
    where
        Self: Send + 'static;
    async fn find_cron_scheduler_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<CronScheduler>>
    where
        Self: Send + 'static;
    async fn find_cron_scheduler_all_list(
        &self,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<CronScheduler>>
    where
        Self: Send + 'static;
    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static;
}
pub struct CronSchedulerAppImpl {
    cron_scheduler_repository: CronSchedulerRepositoryImpl,
    memory_cache: AsyncCache<Arc<String>, CronScheduler>,
    key_lock: RwLockWithKey<Arc<String>>,
    default_ttl: Duration,
    // Phase 4: 通知サービス（コンストラクタ注入）
    notification_service: Arc<dyn ConfigChangeNotificationService>,
}

impl CronSchedulerAppImpl {
    const DEFAULT_TTL_SEC: u64 = 60; // XXX fix it
    pub fn new(
        cron_scheduler_repository: CronSchedulerRepositoryImpl,
        memory_cache: AsyncCache<Arc<String>, CronScheduler>,
        notification_service: Arc<dyn ConfigChangeNotificationService>,
    ) -> Self {
        Self {
            cron_scheduler_repository,
            memory_cache,
            key_lock: RwLockWithKey::new(16 * 1024), // XXX fix it
            default_ttl: Duration::from_secs(Self::DEFAULT_TTL_SEC),
            notification_service,
        }
    }
}

impl UseCronSchedulerRepository for CronSchedulerAppImpl {
    fn cron_scheduler_repository(&self) -> &CronSchedulerRepositoryImpl {
        &self.cron_scheduler_repository
    }
}
#[async_trait]
impl CronSchedulerApp for CronSchedulerAppImpl {
    fn notification_service(&self) -> &Arc<dyn ConfigChangeNotificationService> {
        &self.notification_service
    }

    // Intentionally duplicates gRPC-layer validation so App layer is independently safe
    // (e.g. when called from TOML import without gRPC).
    shared::define_validate_execution_target_app!(
        CronSchedulerData,
        proto::jobworkerp_conductor::data::cron_scheduler_data
    );

    fn find_cache_key(id: &i64) -> String {
        ["cron_scheduler_id:", &id.to_string()].join("")
    }

    fn find_by_name_cache_key(name: &str) -> String {
        ["cron_scheduler_name:", name].join("")
    }

    async fn find_cron_scheduler(
        &self,
        id: &CronSchedulerId,
        ttl: Option<&Duration>,
    ) -> Result<Option<CronScheduler>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_cache_key(&id.value));
        self.with_cache_if_some(&k, ttl, || async {
            self.cron_scheduler_repository().find(id).await
        })
        .await
    }

    async fn find_cron_scheduler_by_name(
        &self,
        name: &str,
        ttl: Option<&Duration>,
    ) -> Result<Option<CronScheduler>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_by_name_cache_key(name));
        self.with_cache_if_some(&k, ttl, || async {
            self.cron_scheduler_repository().find_by_name(name).await
        })
        .await
    }

    async fn find_cron_scheduler_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<CronScheduler>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        self.cron_scheduler_repository()
            .find_list(limit, offset)
            .await
    }

    async fn find_cron_scheduler_all_list(
        &self,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<CronScheduler>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        self.cron_scheduler_repository().find_list(None, None).await
    }

    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static,
    {
        // TODO cache
        self.cron_scheduler_repository()
            .count_list_tx(self.cron_scheduler_repository().db_pool())
            .await
    }

    async fn create_cron_scheduler(
        &self,
        cron_scheduler: &CronSchedulerData,
    ) -> Result<CronSchedulerId> {
        Self::validate_execution_target(cron_scheduler)?;

        let db = self.cron_scheduler_repository().db_pool();
        let mut tx = db.begin().await.map_err(UiEventHandlerError::DBError)?;
        let id = self
            .cron_scheduler_repository()
            .create(&mut *tx, cron_scheduler)
            .await?;
        tx.commit().await.map_err(UiEventHandlerError::DBError)?;

        // 🚀 Phase 4: 作成イベント通知（完全データ送信）
        let event =
            shared::config_events_proto::ConfigChangeEventWrapper::create_cron_scheduler_created(
                cron_scheduler.name.clone(),
                Some(id),
                Some(cron_scheduler.clone()),
                None, // JobworkerpServer情報は必要に応じて追加
            );
        if let Err(e) = self.notification_service().notify(event).await {
            tracing::warn!("Failed to send cron_scheduler created notification: {}", e);
        }

        Ok(id)
    }

    async fn update_cron_scheduler(
        &self,
        id: &CronSchedulerId,
        cron_scheduler: &Option<CronSchedulerData>,
    ) -> Result<bool> {
        if let Some(w) = cron_scheduler {
            Self::validate_execution_target(w)?;
            let pool = self.cron_scheduler_repository().db_pool();
            let mut tx = pool.begin().await.map_err(UiEventHandlerError::DBError)?;
            self.cron_scheduler_repository()
                .update(&mut *tx, id, w)
                .await?;
            tx.commit().await.map_err(UiEventHandlerError::DBError)?;
            // clear memory cache
            let k = Arc::new(Self::find_cache_key(&id.value));
            let _ = self.delete_cache(&k).await;

            // 🚀 Phase 4: 更新イベント通知（完全データ送信）
            let event = shared::config_events_proto::ConfigChangeEventWrapper::create_cron_scheduler_updated(
                w.name.clone(),
                Some(*id),
                Some(w.clone()),
                None,
            );
            if let Err(e) = self.notification_service().notify(event).await {
                tracing::warn!("Failed to send cron_scheduler updated notification: {}", e);
            }

            Ok(true)
        } else {
            // clear memory cache
            let k = Arc::new(Self::find_cache_key(&id.value));
            let _ = self.delete_cache(&k).await;

            // all empty, no update
            Ok(false)
        }
    }

    async fn delete_cron_scheduler(&self, id: &CronSchedulerId) -> Result<bool> {
        // 削除前にデータを取得（通知用）
        let existing = self.cron_scheduler_repository().find(id).await;

        let r = self.cron_scheduler_repository().delete(id).await;

        match r {
            Ok(true) => {
                let k = Arc::new(Self::find_cache_key(&id.value));
                let _ = self.delete_cache(&k).await;

                // 🚀 Phase 4: 削除イベント通知（完全データ送信）
                if let Ok(Some(existing_data)) = existing {
                    let event = shared::config_events_proto::ConfigChangeEventWrapper::create_cron_scheduler_deleted(
                        existing_data.data.as_ref().unwrap().name.clone(),
                        Some(*id)
                    );
                    if let Err(e) = self.notification_service().notify(event).await {
                        tracing::warn!("Failed to send cron_scheduler deleted notification: {}", e);
                    }
                }

                Ok(true)
            }
            other => other,
        }
    }
}

impl UseMemoryCache<Arc<String>, CronScheduler> for CronSchedulerAppImpl {
    fn cache(&self) -> &AsyncCache<Arc<String>, CronScheduler> {
        &self.memory_cache
    }
    #[doc = " default cache ttl"]
    fn default_ttl(&self) -> Option<&Duration> {
        Some(&self.default_ttl)
    }
    fn key_lock(&self) -> &RwLockWithKey<Arc<String>> {
        &self.key_lock
    }
}
