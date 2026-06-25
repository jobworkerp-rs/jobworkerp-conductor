use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use infra_utils::infra::redis::{RedisClient, UseRedisClient};
use shared::config_events_proto::ConfigChangeEventWrapper as ConfigChangeEvent;
use shared::notification::{NotificationReceiver, NotificationRepository};
use std::sync::Arc;

/// Redis通知リポジトリ実装
///
/// RedisのPub/Sub機能を使用した分散通知システム。
/// protobufバイナリシリアライゼーションでマルチノード対応。
pub struct RedisNotificationRepository {
    redis_client: Arc<RedisClient>,
    channel_prefix: String,
}

impl UseRedisClient for RedisNotificationRepository {
    fn redis_client(&self) -> &RedisClient {
        &self.redis_client
    }
}

impl RedisNotificationRepository {
    /// 新しいRedis通知リポジトリを作成
    pub fn new(redis_client: Arc<RedisClient>, channel_prefix: String) -> Self {
        Self {
            redis_client,
            channel_prefix,
        }
    }

    /// デフォルトチャンネルプレフィックスで作成
    pub fn new_with_default_prefix(redis_client: Arc<RedisClient>) -> Self {
        Self::new(redis_client, "conductor_config".to_string())
    }

    /// イベントタイプに応じたチャンネル名を生成（将来の拡張用）
    #[allow(dead_code)]
    fn get_channel_name(&self, action: &str) -> String {
        format!("{}:{}", self.channel_prefix, action)
    }

    /// 統一チャンネル名を生成（全イベント用）
    fn get_unified_channel_name(&self) -> String {
        format!("{}:all", self.channel_prefix)
    }
}

#[async_trait]
impl NotificationRepository for RedisNotificationRepository {
    async fn publish(&self, event: ConfigChangeEvent) -> Result<()> {
        // protobufバイナリにシリアライズ
        let serialized = event
            .to_protobuf_bytes()
            .map_err(|e| anyhow::anyhow!("Failed to serialize event to protobuf: {}", e))?;

        // UseRedisClient traitのpublishメソッドを使用
        let channel = self.get_unified_channel_name();
        let _result = UseRedisClient::publish(self, &channel, &serialized)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to publish to Redis channel {}: {}", channel, e)
            })?;

        tracing::debug!(
            "Published config change event to Redis channel {}: action={:?}, name={}",
            channel,
            event.action(),
            event.entity_name()
        );

        Ok(())
    }

    async fn subscribe(&self) -> Result<Box<dyn NotificationReceiver>> {
        // UseRedisClient traitのsubscribeメソッドを使用
        let channel = self.get_unified_channel_name();
        let pubsub = UseRedisClient::subscribe(self, &channel)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to subscribe to Redis channel {}: {}", channel, e)
            })?;

        tracing::debug!("Subscribed to Redis channel: {}", channel);

        Ok(Box::new(RedisNotificationReceiver { pubsub }))
    }
}

/// Redis通知受信者実装
pub struct RedisNotificationReceiver {
    pubsub: redis::aio::PubSub,
}

#[async_trait]
impl NotificationReceiver for RedisNotificationReceiver {
    async fn receive(&mut self) -> Result<Option<ConfigChangeEvent>> {
        // Redis PubSubから次のメッセージを取得
        match self.pubsub.on_message().next().await {
            Some(msg) => {
                let payload: Vec<u8> = msg
                    .get_payload()
                    .map_err(|e| anyhow::anyhow!("Failed to get message payload: {}", e))?;

                // protobufバイナリからConfigChangeEventにデシリアライズ
                let event = ConfigChangeEvent::from_protobuf_bytes(&payload).map_err(|e| {
                    anyhow::anyhow!("Failed to deserialize event from protobuf: {}", e)
                })?;

                tracing::debug!(
                    "Received config change event from Redis: action={:?}, name={}",
                    event.action(),
                    event.entity_name()
                );

                Ok(Some(event))
            }
            None => {
                tracing::info!("Redis pubsub stream ended");
                Ok(None) // 接続クローズとして扱う
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infra_utils::infra::test::setup_test_redis_client;
    use shared::config_events_proto::ConfigChangeEventWrapper;
    use tokio::time::{timeout, Duration};

    #[test]
    fn test_channel_name_generation() {
        let redis_client = setup_test_redis_client().expect("Failed to setup test Redis client");
        let repo =
            RedisNotificationRepository::new(Arc::new(redis_client), "test_prefix".to_string());

        assert_eq!(repo.get_channel_name("created"), "test_prefix:created");
        assert_eq!(repo.get_channel_name("updated"), "test_prefix:updated");
        assert_eq!(repo.get_unified_channel_name(), "test_prefix:all");
    }

    #[test]
    fn test_default_prefix_creation() {
        let redis_client = setup_test_redis_client().expect("Failed to setup test Redis client");
        let repo = RedisNotificationRepository::new_with_default_prefix(Arc::new(redis_client));

        assert_eq!(repo.get_unified_channel_name(), "conductor_config:all");
    }

    #[test]
    fn test_serialization_roundtrip() {
        use proto::jobworkerp_conductor::data::CronSchedulerId;

        // UnifiedConfigChangeEventのシリアライゼーション/デシリアライゼーションテスト
        let event = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "test_scheduler".to_string(),
            Some(CronSchedulerId { value: 1 }),
            None,
            None,
        );

        // protobufバイナリ変換テスト
        let serialized = event.to_protobuf_bytes().expect("Failed to serialize");
        assert!(!serialized.is_empty());

        let deserialized = ConfigChangeEventWrapper::from_protobuf_bytes(&serialized)
            .expect("Failed to deserialize");

        assert_eq!(event.entity_name(), deserialized.entity_name());
        assert_eq!(
            format!("{:?}", event.action()),
            format!("{:?}", deserialized.action())
        );
    }

    #[tokio::test]
    async fn test_redis_publish_subscribe_integration() {
        // 実際のRedisインスタンスを使用した結合テスト
        let redis_client = setup_test_redis_client().expect("Failed to setup test Redis client");
        let redis_client = Arc::new(redis_client);

        // Publisher用リポジトリ
        let publisher_repo =
            RedisNotificationRepository::new(redis_client.clone(), "test_integration".to_string());

        // Subscriber用リポジトリ
        let subscriber_repo =
            RedisNotificationRepository::new(redis_client.clone(), "test_integration".to_string());

        // サブスクライバー開始
        let mut receiver = NotificationRepository::subscribe(&subscriber_repo)
            .await
            .expect("Failed to subscribe");

        // テストイベント作成
        let test_event = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "integration_test_scheduler".to_string(),
            Some(proto::jobworkerp_conductor::data::CronSchedulerId { value: 123 }),
            None,
            None,
        );

        // イベント発行
        NotificationRepository::publish(&publisher_repo, test_event.clone())
            .await
            .expect("Failed to publish event");

        // イベント受信（タイムアウト付き）
        let received_event = timeout(Duration::from_secs(5), receiver.receive())
            .await
            .expect("Timeout waiting for message")
            .expect("Failed to receive message")
            .expect("Received None message");

        // 受信イベント検証
        assert_eq!(received_event.entity_name(), "integration_test_scheduler");
        assert_eq!(
            format!("{:?}", received_event.action()),
            format!("{:?}", test_event.action())
        );
        assert!(received_event.is_cron_scheduler());
    }

    #[tokio::test]
    async fn test_redis_multiple_events_integration() {
        // 複数イベントの送受信テスト
        let redis_client = setup_test_redis_client().expect("Failed to setup test Redis client");
        let redis_client = Arc::new(redis_client);

        let publisher_repo =
            RedisNotificationRepository::new(redis_client.clone(), "test_multi".to_string());

        let subscriber_repo =
            RedisNotificationRepository::new(redis_client.clone(), "test_multi".to_string());

        let mut receiver = NotificationRepository::subscribe(&subscriber_repo)
            .await
            .expect("Failed to subscribe");

        // 複数のテストイベント作成・送信
        let events = vec![
            ConfigChangeEventWrapper::create_cron_scheduler_created(
                "scheduler_1".to_string(),
                None,
                None,
                None,
            ),
            ConfigChangeEventWrapper::create_cron_scheduler_updated(
                "scheduler_2".to_string(),
                None,
                None,
                None,
            ),
            ConfigChangeEventWrapper::create_cron_scheduler_deleted(
                "scheduler_3".to_string(),
                None,
            ),
        ];

        for event in &events {
            NotificationRepository::publish(&publisher_repo, event.clone())
                .await
                .expect("Failed to publish event");
        }

        // 全イベント受信確認
        for (i, expected_event) in events.iter().enumerate() {
            let received_event = timeout(Duration::from_secs(3), receiver.receive())
                .await
                .unwrap_or_else(|_| panic!("Timeout waiting for message {i}"))
                .unwrap_or_else(|_| panic!("Failed to receive message {i}"))
                .unwrap_or_else(|| panic!("Received None message {i}"));

            assert_eq!(received_event.entity_name(), expected_event.entity_name());
            assert_eq!(
                format!("{:?}", received_event.action()),
                format!("{:?}", expected_event.action())
            );
        }
    }

    #[tokio::test]
    async fn test_redis_connection_failure_handling() {
        // 無効なRedis URLでの接続失敗テスト
        let invalid_client = RedisClient::open("redis://invalid-host:6379").unwrap();
        let repo =
            RedisNotificationRepository::new(Arc::new(invalid_client), "test_failure".to_string());

        let test_event = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "test".to_string(),
            None,
            None,
            None,
        );

        // 発行時の接続エラーを確認
        let publish_result = NotificationRepository::publish(&repo, test_event).await;
        assert!(
            publish_result.is_err(),
            "Expected publish to fail with invalid Redis connection"
        );

        // 購読時の接続エラーを確認
        let subscribe_result = NotificationRepository::subscribe(&repo).await;
        assert!(
            subscribe_result.is_err(),
            "Expected subscribe to fail with invalid Redis connection"
        );
    }
}
