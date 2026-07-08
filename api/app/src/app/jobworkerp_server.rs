use anyhow::Result;
use async_trait::async_trait;
use infra::error::UiEventHandlerError;
use infra::infra::jobworkerp_server::rdb::{
    JobworkerpServerRepository, JobworkerpServerRepositoryImpl, UseJobworkerpServerRepository,
};
use infra_utils::infra::rdb::UseRdbPool;
use memory_utils::cache::stretto::UseMemoryCache;
use memory_utils::lock::RwLockWithKey;
use proto::jobworkerp_conductor::data::{
    JobworkerpServer, JobworkerpServerData, JobworkerpServerId,
};
use shared::notification::service::ConfigChangeNotificationService;
use std::{sync::Arc, time::Duration};
use stretto::TokioCache;

#[async_trait]
pub trait JobworkerpServerApp:
    UseJobworkerpServerRepository
    + UseMemoryCache<Arc<String>, JobworkerpServer>
    + Send
    + Sync
    + Sized
    + 'static
{
    async fn create_jobworkerp_server(
        &self,
        jobworkerp_server: &JobworkerpServerData,
    ) -> Result<JobworkerpServerId>;
    async fn update_jobworkerp_server(
        &self,
        id: &JobworkerpServerId,
        jobworkerp_server: &Option<JobworkerpServerData>,
    ) -> Result<bool>;
    async fn delete_jobworkerp_server(&self, id: &JobworkerpServerId) -> Result<bool>;
    fn find_cache_key(id: &i64) -> String;
    fn find_by_name_cache_key(name: &str) -> String;
    async fn find_jobworkerp_server(
        &self,
        id: &JobworkerpServerId,
        ttl: Option<&Duration>,
    ) -> Result<Option<JobworkerpServer>>
    where
        Self: Send + 'static;
    async fn find_jobworkerp_server_by_name(
        &self,
        name: &str,
        ttl: Option<&Duration>,
    ) -> Result<Option<JobworkerpServer>>
    where
        Self: Send + 'static;
    async fn find_jobworkerp_server_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<JobworkerpServer>>
    where
        Self: Send + 'static;
    async fn find_jobworkerp_server_all_list(
        &self,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<JobworkerpServer>>
    where
        Self: Send + 'static;
    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static;
}
pub struct JobworkerpServerAppImpl {
    jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
    memory_cache: TokioCache<Arc<String>, JobworkerpServer>,
    key_lock: RwLockWithKey<Arc<String>>,
    default_ttl: Duration,
    // 🚀 Phase 4: 通知サービス（コンストラクタ注入）
    notification_service: Arc<dyn ConfigChangeNotificationService>,
}

impl JobworkerpServerAppImpl {
    const DEFAULT_TTL_SEC: u64 = 60; // XXX fix it
    pub fn new(
        jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
        memory_cache: TokioCache<Arc<String>, JobworkerpServer>,
        notification_service: Arc<dyn ConfigChangeNotificationService>,
    ) -> Self {
        Self {
            jobworkerp_server_repository,
            memory_cache,
            key_lock: RwLockWithKey::new(16 * 1024), // XXX fix it
            default_ttl: Duration::from_secs(Self::DEFAULT_TTL_SEC),
            notification_service,
        }
    }
}

impl UseJobworkerpServerRepository for JobworkerpServerAppImpl {
    fn jobworkerp_server_repository(&self) -> &JobworkerpServerRepositoryImpl {
        &self.jobworkerp_server_repository
    }
}
#[async_trait]
impl JobworkerpServerApp for JobworkerpServerAppImpl {
    fn find_cache_key(id: &i64) -> String {
        ["jobworkerp_server_id:", &id.to_string()].join("")
    }

    fn find_by_name_cache_key(name: &str) -> String {
        ["jobworkerp_server_name:", name].join("")
    }

    async fn find_jobworkerp_server(
        &self,
        id: &JobworkerpServerId,
        ttl: Option<&Duration>,
    ) -> Result<Option<JobworkerpServer>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_cache_key(&id.value));
        self.with_cache_if_some(&k, ttl, || async {
            self.jobworkerp_server_repository().find(id).await
        })
        .await
    }

    async fn find_jobworkerp_server_by_name(
        &self,
        name: &str,
        ttl: Option<&Duration>,
    ) -> Result<Option<JobworkerpServer>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_by_name_cache_key(name));
        self.with_cache_if_some(&k, ttl, || async {
            self.jobworkerp_server_repository().find_by_name(name).await
        })
        .await
    }

    async fn find_jobworkerp_server_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<JobworkerpServer>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        self.jobworkerp_server_repository()
            .find_list(limit, offset)
            .await
    }

    async fn find_jobworkerp_server_all_list(
        &self,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<JobworkerpServer>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        self.jobworkerp_server_repository()
            .find_list(None, None)
            .await
    }

    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static,
    {
        // TODO cache
        self.jobworkerp_server_repository()
            .count_list_tx(self.jobworkerp_server_repository().db_pool())
            .await
    }

    // 🚀 Phase 4: create時の通知機能追加
    async fn create_jobworkerp_server(
        &self,
        jobworkerp_server: &JobworkerpServerData,
    ) -> Result<JobworkerpServerId> {
        // transaction example
        let db = self.jobworkerp_server_repository().db_pool();
        let mut tx = db.begin().await.map_err(UiEventHandlerError::DBError)?;
        let id = self
            .jobworkerp_server_repository()
            .create(&mut *tx, jobworkerp_server)
            .await?;
        tx.commit().await.map_err(UiEventHandlerError::DBError)?;

        // 🚀 Phase 4: 作成イベント通知
        let event =
            shared::config_events_proto::ConfigChangeEventWrapper::create_jobworkerp_server_created(
                jobworkerp_server.name.clone(),
                id,
                Some(jobworkerp_server.clone()),
            );
        if let Err(e) = self.notification_service.notify(event).await {
            tracing::warn!(
                "Failed to send jobworkerp_server created notification: {}",
                e
            );
        }

        Ok(id)
    }

    // 🚀 Phase 4: update時の通知機能追加
    async fn update_jobworkerp_server(
        &self,
        id: &JobworkerpServerId,
        jobworkerp_server: &Option<JobworkerpServerData>,
    ) -> Result<bool> {
        if let Some(w) = jobworkerp_server {
            let pool = self.jobworkerp_server_repository().db_pool();
            let mut tx = pool.begin().await.map_err(UiEventHandlerError::DBError)?;
            self.jobworkerp_server_repository()
                .update(&mut *tx, id, w)
                .await?;
            tx.commit().await.map_err(UiEventHandlerError::DBError)?;
            // clear memory cache
            let k = Arc::new(Self::find_cache_key(&id.value));
            let _ = self.delete_cache(&k).await;

            // 🚀 Phase 4: 更新イベント通知
            let event =
                shared::config_events_proto::ConfigChangeEventWrapper::create_jobworkerp_server_updated(w.name.clone(), *id, Some(w.clone()),
                );
            if let Err(e) = self.notification_service.notify(event).await {
                tracing::warn!(
                    "Failed to send jobworkerp_server updated notification: {}",
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
    async fn delete_jobworkerp_server(&self, id: &JobworkerpServerId) -> Result<bool> {
        // 削除前にデータを取得（通知用）
        let existing = self.jobworkerp_server_repository().find(id).await;

        let r = self.jobworkerp_server_repository().delete(id).await;

        match r {
            Ok(true) => {
                let k = Arc::new(Self::find_cache_key(&id.value));
                let _ = self.delete_cache(&k).await;

                // 🚀 Phase 4: 削除イベント通知
                if let Ok(Some(existing_data)) = existing {
                    let event = shared::config_events_proto::ConfigChangeEventWrapper::create_jobworkerp_server_deleted(existing_data.data.as_ref().unwrap().name.clone(), *id, None
                    );
                    if let Err(e) = self.notification_service.notify(event).await {
                        tracing::warn!(
                            "Failed to send jobworkerp_server deleted notification: {}",
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

impl UseMemoryCache<Arc<String>, JobworkerpServer> for JobworkerpServerAppImpl {
    fn cache(&self) -> &TokioCache<Arc<String>, JobworkerpServer> {
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
