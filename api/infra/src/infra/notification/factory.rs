use super::{channel::ChannelNotificationRepository, redis::RedisNotificationRepository};
use anyhow::Result;
use shared::notification::NotificationRepository;
use std::sync::Arc;

/// 通知設定オプション（Phase 1: Channel/Redis両対応）
#[derive(Debug, Clone)]
pub enum NotificationConfig {
    /// メモリ内チャンネル通知（単一ノード構成）
    Channel { buffer_size: usize },
    /// Redis Pub/Sub通知（分散ノード構成）
    Redis {
        channel_prefix: String,
        redis_url: String,
    },
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self::Channel { buffer_size: 1000 }
    }
}

/// NotificationRepository ファクトリー
///
/// 設定に応じて適切な NotificationRepository 実装を作成。
/// 依存性注入パターンによる柔軟な実装選択を支援。
#[derive(Clone)]
pub struct NotificationRepositoryFactory;

impl NotificationRepositoryFactory {
    /// 設定から NotificationRepository を作成
    pub fn create(config: NotificationConfig) -> Result<Box<dyn NotificationRepository>> {
        match config {
            NotificationConfig::Channel { buffer_size } => {
                tracing::info!(
                    "Creating Channel notification repository with buffer size: {}",
                    buffer_size
                );
                Ok(Box::new(ChannelNotificationRepository::new(buffer_size)))
            }
            NotificationConfig::Redis {
                channel_prefix,
                redis_url,
            } => {
                tracing::warn!("Redis notification requires pre-configured client for config-based creation, use create_redis() method instead. Falling back to Channel with prefix: {}", channel_prefix);
                Ok(Box::new(RedisNotificationRepository::new(
                    Arc::new(
                        infra_utils::infra::redis::RedisClient::open(redis_url)
                            .map_err(|e| anyhow::anyhow!("Failed to open Redis client: {}", e))?,
                    ),
                    channel_prefix,
                )))
            }
        }
    }

    /// 環境変数から設定を読み込んで NotificationRepository を作成
    pub fn create_by_env() -> Result<Box<dyn NotificationRepository>> {
        let notification_type =
            std::env::var("NOTIFICATION_TYPE").unwrap_or_else(|_| "channel".to_string());

        match notification_type.to_lowercase().as_str() {
            "redis" => {
                let channel_prefix = std::env::var("NOTIFICATION_CHANNEL_PREFIX")
                    .unwrap_or_else(|_| "conductor_config".to_string());

                // Redis URLが設定されているかチェック
                if let Ok(url) = std::env::var("REDIS_URL") {
                    // Redis client creation is complex and requires proper setup
                    // For now, fall back to Channel notification
                    tracing::warn!("Redis notification configuration not yet fully implemented, falling back to Channel");

                    Ok(Box::new(RedisNotificationRepository::new(
                        Arc::new(infra_utils::infra::redis::RedisClient::open(url)?),
                        channel_prefix,
                    )))
                } else {
                    tracing::warn!("REDIS_URL not set, falling back to Channel notification");
                    Err(anyhow::anyhow!(
                        "REDIS_URL environment variable is required for Redis notification"
                    ))
                }
            }
            _ => {
                let buffer_size = std::env::var("NOTIFICATION_BUFFER_SIZE")
                    .unwrap_or_else(|_| "1000".to_string())
                    .parse::<usize>()
                    .unwrap_or(1000);

                tracing::info!(
                    "Creating Channel notification with buffer size: {}",
                    buffer_size
                );
                Ok(Box::new(ChannelNotificationRepository::new(buffer_size)))
            }
        }
    }
    /// デフォルト設定でチャンネル通知を作成
    pub fn create_default_channel() -> Result<Box<dyn NotificationRepository>> {
        Self::create(NotificationConfig::default())
    }

    /// Redis通知を作成（事前設定済みクライアント使用）
    /// 本格的なRedis通知実装（Phase 1完成）
    pub fn create_redis(
        redis_client: Arc<infra_utils::infra::redis::RedisClient>,
        channel_prefix: Option<String>,
    ) -> Box<dyn NotificationRepository> {
        let prefix = channel_prefix.unwrap_or_else(|| "conductor_config".to_string());
        tracing::info!(
            "Creating Redis notification repository with prefix: {}",
            prefix
        );
        Box::new(RedisNotificationRepository::new(redis_client, prefix))
    }
}
