use anyhow::Result;
use async_trait::async_trait;
use infra::infra::notification::{NotificationConfig, NotificationRepositoryFactory};
use shared::config_events_proto::ConfigChangeEventWrapper as ConfigChangeEvent;
use shared::notification::{
    ConfigChangeEventReceiver, ConfigChangeNotificationService, NotificationReceiverAdapter,
    NotificationRepository,
};
use std::sync::Arc;

/// UI Event Handler層の設定変更通知サービス実装
///
/// インフラ層のNotificationRepositoryを使用してビジネスロジック層の
/// 通知サービスを実装。依存関係逆転によりテスト容易性を確保。
#[derive(Clone)]
pub struct ConfigChangeNotificationServiceImpl {
    repository: Arc<dyn NotificationRepository>,
}

impl ConfigChangeNotificationServiceImpl {
    /// NotificationRepository を使用して作成
    pub fn new(repository: Arc<dyn NotificationRepository>) -> Self {
        Self { repository }
    }

    pub fn new_by_env() -> Result<Self> {
        let repository = NotificationRepositoryFactory::create_by_env()?;
        Ok(Self::new(Arc::from(repository)))
    }

    /// メモリベース通知で作成
    pub fn new_memory(capacity: usize) -> Result<Self> {
        let config = NotificationConfig::Channel {
            buffer_size: capacity,
        };
        let repository = NotificationRepositoryFactory::create(config)?;
        Ok(Self::new(Arc::from(repository)))
    }

    /// デフォルトメモリベース通知で作成
    pub fn new_memory_default() -> Result<Self> {
        Self::new_memory(1000)
    }

    /// 環境変数から設定を読み込んで作成
    pub fn create_by_config() -> Result<Self> {
        let repository = NotificationRepositoryFactory::create_by_env()?;
        Ok(Self::new(Arc::from(repository)))
    }

    /// Redis通知で作成
    pub fn new_redis(
        redis_client: Arc<infra_utils::infra::redis::RedisClient>,
        channel_prefix: Option<String>,
    ) -> Self {
        let repository = NotificationRepositoryFactory::create_redis(redis_client, channel_prefix);
        Self::new(Arc::from(repository))
    }
}

#[async_trait]
impl ConfigChangeNotificationService for ConfigChangeNotificationServiceImpl {
    async fn notify(&self, event: ConfigChangeEvent) -> Result<()> {
        tracing::debug!(
            "Notifying configuration change: action={:?}, name={}",
            event.action(),
            event.entity_name()
        );

        self.repository.publish(event).await
    }

    async fn subscribe(&self) -> Result<Box<dyn ConfigChangeEventReceiver>> {
        tracing::debug!("Creating new configuration change event subscriber");
        let receiver = self.repository.subscribe().await?;
        Ok(Box::new(NotificationReceiverAdapter::new(receiver)))
    }
}

pub struct NotificationServiceFactory;

impl NotificationServiceFactory {
    /// メモリベース通知サービスを作成
    pub fn create_memory_service(capacity: usize) -> Result<ConfigChangeNotificationServiceImpl> {
        tracing::info!(
            "Creating memory-based notification service with capacity: {}",
            capacity
        );
        ConfigChangeNotificationServiceImpl::new_memory(capacity)
    }

    /// デフォルトメモリベース通知サービスを作成
    pub fn create_default_memory_service() -> Result<ConfigChangeNotificationServiceImpl> {
        tracing::info!("Creating default memory-based notification service");
        ConfigChangeNotificationServiceImpl::new_memory_default()
    }

    /// 環境変数から設定を読み込んで通知サービスを作成
    pub fn create_from_config() -> Result<ConfigChangeNotificationServiceImpl> {
        tracing::info!("Creating notification service from environment configuration");
        ConfigChangeNotificationServiceImpl::create_by_config()
    }

    /// Redis通知サービスを作成
    pub fn create_redis_service(
        redis_client: Arc<infra_utils::infra::redis::RedisClient>,
        channel_prefix: Option<String>,
    ) -> ConfigChangeNotificationServiceImpl {
        tracing::info!("Creating Redis-based notification service");
        ConfigChangeNotificationServiceImpl::new_redis(redis_client, channel_prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_notification_service_impl() {
        let service = ConfigChangeNotificationServiceImpl::new_memory_default().unwrap();

        let mut receiver = service.subscribe().await.unwrap();

        let test_event = ConfigChangeEvent::test_event("test_service".to_string());

        let notify_service = service.clone();
        let notify_event = test_event.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            notify_service.notify(notify_event).await.unwrap();
        });

        let received = timeout(Duration::from_secs(1), receiver.receive())
            .await
            .expect("Timeout waiting for event")
            .expect("Failed to receive event")
            .expect("No event received");

        assert_eq!(received.action(), test_event.action());
        assert_eq!(received.entity_name(), test_event.entity_name());
    }

    #[tokio::test]
    async fn test_notification_service_factory() {
        let service = NotificationServiceFactory::create_default_memory_service().unwrap();

        let event = ConfigChangeEvent::test_cron_scheduler_created("factory_test".to_string());
        assert!(service.notify(event).await.is_ok());
    }

    #[tokio::test]
    async fn test_notification_service_from_config() {
        let service = NotificationServiceFactory::create_from_config().unwrap();

        let event = ConfigChangeEvent::test_event("config_test".to_string());
        assert!(service.notify(event).await.is_ok());
    }
}
