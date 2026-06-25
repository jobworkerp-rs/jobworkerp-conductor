use super::repository::NotificationReceiver;
use super::service::ConfigChangeEventReceiver;
use crate::config_events_proto::ConfigChangeEventWrapper as ConfigChangeEvent;
use anyhow::Result;
use async_trait::async_trait;

/// NotificationReceiver から ConfigChangeEventReceiver へのアダプター
///
/// インフラ層の NotificationReceiver をアプリケーション層の
/// ConfigChangeEventReceiver として使用するためのアダプター。
/// 依存関係逆転の実現を支援。
pub struct NotificationReceiverAdapter {
    inner: Box<dyn NotificationReceiver>,
}

impl NotificationReceiverAdapter {
    pub fn new(receiver: Box<dyn NotificationReceiver>) -> Self {
        Self { inner: receiver }
    }
}

#[async_trait]
impl ConfigChangeEventReceiver for NotificationReceiverAdapter {
    async fn receive(&mut self) -> Result<Option<ConfigChangeEvent>> {
        self.inner.receive().await
    }
}
