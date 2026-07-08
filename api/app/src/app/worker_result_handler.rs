use anyhow::Result;
use async_trait::async_trait;
use infra::error::UiEventHandlerError;
use infra::infra::worker_result_handler::rdb::{
    UseWorkerResultHandlerRepository, WorkerResultHandlerRepository,
    WorkerResultHandlerRepositoryImpl,
};
use infra_utils::infra::rdb::UseRdbPool;
use memory_utils::cache::stretto::UseMemoryCache;
use memory_utils::lock::RwLockWithKey;
use proto::jobworkerp_conductor::data::{
    WorkerResultHandler, WorkerResultHandlerData, WorkerResultHandlerId,
};
use shared::notification::service::ConfigChangeNotificationService;
use std::{sync::Arc, time::Duration};
use stretto::TokioCache;

#[async_trait]
pub trait WorkerResultHandlerApp:
    UseWorkerResultHandlerRepository
    + UseMemoryCache<Arc<String>, WorkerResultHandler>
    + Send
    + Sync
    + Sized
    + 'static
{
    fn validate_execution_target(data: &WorkerResultHandlerData) -> Result<()>;
    async fn create_worker_result_handler(
        &self,
        worker_result_handler: &WorkerResultHandlerData,
    ) -> Result<WorkerResultHandlerId>;
    async fn update_worker_result_handler(
        &self,
        id: &WorkerResultHandlerId,
        worker_result_handler: &Option<WorkerResultHandlerData>,
    ) -> Result<bool>;
    async fn delete_worker_result_handler(&self, id: &WorkerResultHandlerId) -> Result<bool>;
    fn find_cache_key(id: &i64) -> String;
    fn find_by_name_cache_key(name: &str) -> String;
    async fn find_worker_result_handler(
        &self,
        id: &WorkerResultHandlerId,
        ttl: Option<&Duration>,
    ) -> Result<Option<WorkerResultHandler>>
    where
        Self: Send + 'static;
    async fn find_worker_result_handler_by_name(
        &self,
        name: &str,
        ttl: Option<&Duration>,
    ) -> Result<Option<WorkerResultHandler>>
    where
        Self: Send + 'static;
    async fn find_worker_result_handler_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<WorkerResultHandler>>
    where
        Self: Send + 'static;
    async fn find_worker_result_handler_all_list(
        &self,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<WorkerResultHandler>>
    where
        Self: Send + 'static;
    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static;
}
pub struct WorkerResultHandlerAppImpl {
    worker_result_handler_repository: WorkerResultHandlerRepositoryImpl,
    memory_cache: TokioCache<Arc<String>, WorkerResultHandler>,
    key_lock: RwLockWithKey<Arc<String>>,
    default_ttl: Duration,
    // 🚀 Phase 4: 通知サービス（コンストラクタ注入）
    notification_service: Arc<dyn ConfigChangeNotificationService>,
}

impl WorkerResultHandlerAppImpl {
    const DEFAULT_TTL_SEC: u64 = 60; // XXX fix it
    pub fn new(
        worker_result_handler_repository: WorkerResultHandlerRepositoryImpl,
        memory_cache: TokioCache<Arc<String>, WorkerResultHandler>,
        notification_service: Arc<dyn ConfigChangeNotificationService>,
    ) -> Self {
        Self {
            worker_result_handler_repository,
            memory_cache,
            key_lock: RwLockWithKey::new(16 * 1024), // XXX fix it
            default_ttl: Duration::from_secs(Self::DEFAULT_TTL_SEC),
            notification_service,
        }
    }
}

impl UseWorkerResultHandlerRepository for WorkerResultHandlerAppImpl {
    fn worker_result_handler_repository(&self) -> &WorkerResultHandlerRepositoryImpl {
        &self.worker_result_handler_repository
    }
}
#[async_trait]
impl WorkerResultHandlerApp for WorkerResultHandlerAppImpl {
    // Intentionally duplicates gRPC-layer validation so App layer is independently safe
    // (e.g. when called from TOML import without gRPC).
    shared::define_validate_execution_target_app!(
        WorkerResultHandlerData,
        proto::jobworkerp_conductor::data::worker_result_handler_data
    );

    fn find_cache_key(id: &i64) -> String {
        ["worker_result_handler_id:", &id.to_string()].join("")
    }

    fn find_by_name_cache_key(name: &str) -> String {
        ["worker_result_handler_name:", name].join("")
    }

    async fn find_worker_result_handler(
        &self,
        id: &WorkerResultHandlerId,
        ttl: Option<&Duration>,
    ) -> Result<Option<WorkerResultHandler>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_cache_key(&id.value));
        self.with_cache_if_some(&k, ttl, || async {
            self.worker_result_handler_repository().find(id).await
        })
        .await
    }

    async fn find_worker_result_handler_by_name(
        &self,
        name: &str,
        ttl: Option<&Duration>,
    ) -> Result<Option<WorkerResultHandler>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_by_name_cache_key(name));
        self.with_cache_if_some(&k, ttl, || async {
            self.worker_result_handler_repository()
                .find_by_name(name)
                .await
        })
        .await
    }

    async fn find_worker_result_handler_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<WorkerResultHandler>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        self.worker_result_handler_repository()
            .find_list(limit, offset)
            .await
    }

    async fn find_worker_result_handler_all_list(
        &self,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<WorkerResultHandler>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        self.worker_result_handler_repository()
            .find_list(None, None)
            .await
    }

    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static,
    {
        // TODO cache
        self.worker_result_handler_repository()
            .count_list_tx(self.worker_result_handler_repository().db_pool())
            .await
    }

    // 🚀 Phase 4: create時の通知機能追加
    async fn create_worker_result_handler(
        &self,
        worker_result_handler: &WorkerResultHandlerData,
    ) -> Result<WorkerResultHandlerId> {
        Self::validate_execution_target(worker_result_handler)?;

        let db = self.worker_result_handler_repository().db_pool();
        let mut tx = db.begin().await.map_err(UiEventHandlerError::DBError)?;
        let id = self
            .worker_result_handler_repository()
            .create(&mut *tx, worker_result_handler)
            .await?;
        tx.commit().await.map_err(UiEventHandlerError::DBError)?;

        // 🚀 Phase 4: 作成イベント通知
        let event = shared::config_events_proto::ConfigChangeEventWrapper::create_worker_result_handler_created(
            worker_result_handler.name.clone(),
            Some(id),
            Some(worker_result_handler.clone()),
            std::collections::HashMap::new(), // JobworkerpServers情報は必要に応じて追加
        );
        if let Err(e) = self.notification_service.notify(event).await {
            tracing::warn!(
                "Failed to send worker_result_handler created notification: {}",
                e
            );
        }

        Ok(id)
    }

    // 🚀 Phase 4: update時の通知機能追加
    async fn update_worker_result_handler(
        &self,
        id: &WorkerResultHandlerId,
        worker_result_handler: &Option<WorkerResultHandlerData>,
    ) -> Result<bool> {
        if let Some(w) = worker_result_handler {
            Self::validate_execution_target(w)?;
            let pool = self.worker_result_handler_repository().db_pool();
            let mut tx = pool.begin().await.map_err(UiEventHandlerError::DBError)?;
            self.worker_result_handler_repository()
                .update(&mut *tx, id, w)
                .await?;
            tx.commit().await.map_err(UiEventHandlerError::DBError)?;
            // clear memory cache
            let k = Arc::new(Self::find_cache_key(&id.value));
            let _ = self.delete_cache(&k).await;

            // 🚀 Phase 4: 更新イベント通知
            let event = shared::config_events_proto::ConfigChangeEventWrapper::create_worker_result_handler_updated(
                w.name.clone(),
                Some(*id),
                Some(w.clone()),
                std::collections::HashMap::new(),
            );
            if let Err(e) = self.notification_service.notify(event).await {
                tracing::warn!(
                    "Failed to send worker_result_handler updated notification: {}",
                    e
                );
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

    // 🚀 Phase 4: delete時の通知機能追加
    async fn delete_worker_result_handler(&self, id: &WorkerResultHandlerId) -> Result<bool> {
        // 削除前にデータを取得（通知用）
        let existing = self.worker_result_handler_repository().find(id).await;

        let r = self.worker_result_handler_repository().delete(id).await;

        match r {
            Ok(true) => {
                let k = Arc::new(Self::find_cache_key(&id.value));
                let _ = self.delete_cache(&k).await;

                // 🚀 Phase 4: 削除イベント通知
                if let Ok(Some(existing_data)) = existing {
                    let event = shared::config_events_proto::ConfigChangeEventWrapper::create_worker_result_handler_deleted(
                        existing_data.data.as_ref().unwrap().name.clone(),
                        Some(*id),
                        existing_data.data.clone(),
                        std::collections::HashMap::new(),
                    );
                    if let Err(e) = self.notification_service.notify(event).await {
                        tracing::warn!(
                            "Failed to send worker_result_handler deleted notification: {}",
                            e
                        );
                    }
                }

                Ok(true)
            }
            other => other,
        }
    }
}

impl UseMemoryCache<Arc<String>, WorkerResultHandler> for WorkerResultHandlerAppImpl {
    fn cache(&self) -> &TokioCache<Arc<String>, WorkerResultHandler> {
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
