//! 通知システム抽象化モジュール
//!
//! クリーンアーキテクチャに基づく通知システムの抽象化を提供。
//! 依存関係逆転原則に従い、インフラ層がこれらのtraitを実装する。

pub mod receiver;
pub mod repository;
pub mod service;

// 公開API
pub use receiver::NotificationReceiverAdapter;
pub use repository::{NotificationReceiver, NotificationRepository};
pub use service::{ConfigChangeEventReceiver, ConfigChangeNotificationService};

use crate::config_events_proto::ConfigChangeEventWrapper as ConfigChangeEvent;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;

/// メモリベース通知サービス（Phase 0プロトタイプ実装）
///
/// BroadcastChanベースの実装。本番環境では api/infra の実装を使用。
/// プロトタイプおよびテスト目的で提供。
#[derive(Clone)]
pub struct MemoryNotificationService {
    sender: Arc<broadcast::Sender<ConfigChangeEvent>>,
}

impl MemoryNotificationService {
    /// 新しいメモリ通知サービスを作成
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender: Arc::new(sender),
        }
    }

    /// デフォルト容量（1000）で作成
    pub fn new_default() -> Self {
        Self::new(1000)
    }

    /// アクティブな受信者数を取得
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// キューサイズを取得
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    /// キューが空かどうかを確認
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }
}

#[async_trait]
impl ConfigChangeNotificationService for MemoryNotificationService {
    async fn notify(&self, event: ConfigChangeEvent) -> Result<()> {
        match self.sender.send(event) {
            Ok(_) => Ok(()),
            Err(broadcast::error::SendError(_)) => {
                // 受信者がいない場合は正常終了
                tracing::debug!("No receivers for notification event");
                Ok(())
            }
        }
    }

    async fn subscribe(&self) -> Result<Box<dyn ConfigChangeEventReceiver>> {
        let receiver = self.sender.subscribe();
        Ok(Box::new(MemoryEventReceiver { receiver }))
    }
}

/// メモリベースイベント受信者
pub struct MemoryEventReceiver {
    receiver: broadcast::Receiver<ConfigChangeEvent>,
}

#[async_trait]
impl ConfigChangeEventReceiver for MemoryEventReceiver {
    async fn receive(&mut self) -> Result<Option<ConfigChangeEvent>> {
        match self.receiver.recv().await {
            Ok(event) => Ok(Some(event)),
            Err(broadcast::error::RecvError::Closed) => Ok(None),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!("Event receiver lagged, skipped {} events", skipped);
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
    async fn test_memory_notification_service() {
        let service = MemoryNotificationService::new_default();

        // 受信者なしで通知
        let event = ConfigChangeEvent::test_event("test1".to_string());
        assert!(service.notify(event).await.is_ok());
        assert_eq!(service.receiver_count(), 0);

        // 受信者を作成
        let mut receiver = service.subscribe().await.unwrap();
        assert_eq!(service.receiver_count(), 1);

        // イベント送信・受信テスト
        let test_event = ConfigChangeEvent::test_event("test2".to_string());
        let notify_service = service.clone();
        let notify_event = test_event.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
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
    async fn test_multiple_receivers() {
        let service = MemoryNotificationService::new_default();

        // 複数の受信者を作成
        let mut receiver1 = service.subscribe().await.unwrap();
        let mut receiver2 = service.subscribe().await.unwrap();
        let mut receiver3 = service.subscribe().await.unwrap();

        assert_eq!(service.receiver_count(), 3);

        // イベント送信
        let test_event = ConfigChangeEvent::test_cron_scheduler_created("scheduler1".to_string());
        service.notify(test_event.clone()).await.unwrap();

        // 全受信者がイベントを受信
        let event1 = receiver1.receive().await.unwrap().unwrap();
        let event2 = receiver2.receive().await.unwrap().unwrap();
        let event3 = receiver3.receive().await.unwrap().unwrap();

        assert_eq!(event1.entity_name(), "scheduler1");
        assert_eq!(event2.entity_name(), "scheduler1");
        assert_eq!(event3.entity_name(), "scheduler1");
        assert_eq!(
            event1.action(),
            proto::jobworkerp_conductor::data::ChangeAction::Created
        );
    }

    #[tokio::test]
    async fn test_receiver_dropped() {
        let service = MemoryNotificationService::new_default();

        {
            let _receiver = service.subscribe().await.unwrap();
            assert_eq!(service.receiver_count(), 1);
        } // receiver がドロップされる

        // 受信者がドロップされた後でも通知は成功
        let event = ConfigChangeEvent::test_event("test_drop".to_string());
        assert!(service.notify(event).await.is_ok());
    }
}
