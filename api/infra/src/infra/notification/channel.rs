use anyhow::Result;
use async_trait::async_trait;
use memory_utils::chan::broadcast::BroadcastChan;
use shared::config_events_proto::ConfigChangeEventWrapper as ConfigChangeEvent;
use shared::notification::{NotificationReceiver, NotificationRepository};
use std::sync::Arc;

/// Channel通知リポジトリ実装
///
/// memory-utilsのBroadcastChanを使用したメモリベース通知システム。
/// JSONシリアライゼーションを使用（Phase 0プロトタイプ）。
pub struct ChannelNotificationRepository {
    // BroadcastChanを使用してメモリ内通知
    broadcast_chan: Arc<BroadcastChan<Vec<u8>>>,
}

impl ChannelNotificationRepository {
    /// 新しいChannel通知リポジトリを作成
    pub fn new(buffer_size: usize) -> Self {
        Self {
            broadcast_chan: Arc::new(BroadcastChan::new(buffer_size)),
        }
    }

    /// デフォルトバッファサイズで作成
    pub fn new_default() -> Self {
        Self::new(1000)
    }
}

#[async_trait]
impl NotificationRepository for ChannelNotificationRepository {
    async fn publish(&self, event: ConfigChangeEvent) -> Result<()> {
        // ConfigChangeEventをprotobufバイナリにシリアライズ（Phase 1）
        let serialized = event
            .to_protobuf_bytes()
            .map_err(|e| anyhow::anyhow!("Failed to serialize event: {}", e))?;

        self.broadcast_chan
            .send(serialized)
            .map_err(|e| anyhow::anyhow!("Failed to send notification: {}", e))?;

        tracing::debug!(
            "Published config change event via channel: action={:?}, name={}",
            event.action(),
            event.entity_name()
        );
        Ok(())
    }

    async fn subscribe(&self) -> Result<Box<dyn NotificationReceiver>> {
        let receiver = self.broadcast_chan.receiver().await;

        Ok(Box::new(ChannelNotificationReceiver { receiver }))
    }
}

/// Channel通知受信者実装
pub struct ChannelNotificationReceiver {
    receiver: tokio::sync::broadcast::Receiver<Vec<u8>>,
}

#[async_trait]
impl NotificationReceiver for ChannelNotificationReceiver {
    async fn receive(&mut self) -> Result<Option<ConfigChangeEvent>> {
        match self.receiver.recv().await {
            Ok(serialized_event) => {
                // protobufバイナリからConfigChangeEventにデシリアライズ（Phase 1）
                let event = ConfigChangeEvent::from_protobuf_bytes(&serialized_event)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize event: {}", e))?;

                tracing::debug!(
                    "Received config change event via channel: action={:?}, name={}",
                    event.action(),
                    event.entity_name()
                );
                Ok(Some(event))
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                tracing::info!("Channel notification receiver closed");
                Ok(None)
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!("Channel receiver lagged, skipped {} events", skipped);
                // 次のイベントを受信
                self.receive().await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_channel_notification_repository() {
        let repo = ChannelNotificationRepository::new_default();

        // 受信者を作成
        let mut receiver = repo.subscribe().await.unwrap();

        // イベント送信・受信テスト
        let test_event = ConfigChangeEvent::create_cron_scheduler_created(
            "channel_test".to_string(),
            None,
            None,
            None,
        );

        let publish_repo = repo;
        let publish_event = test_event.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            publish_repo.publish(publish_event).await.unwrap();
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
    async fn test_multiple_subscribers() {
        let repo = ChannelNotificationRepository::new_default();

        // 複数の受信者を作成
        let mut receiver1 = repo.subscribe().await.unwrap();
        let mut receiver2 = repo.subscribe().await.unwrap();

        // イベント送信
        let test_event = ConfigChangeEvent::create_cron_scheduler_created(
            "multi_test".to_string(),
            None,
            None,
            None,
        );
        repo.publish(test_event.clone()).await.unwrap();

        // 全受信者がイベントを受信
        let event1 = receiver1.receive().await.unwrap().unwrap();
        let event2 = receiver2.receive().await.unwrap().unwrap();

        assert_eq!(event1.entity_name(), "multi_test");
        assert_eq!(event2.entity_name(), "multi_test");
        assert_eq!(
            event1.action(),
            proto::jobworkerp_conductor::data::ChangeAction::Created
        );
    }
}
