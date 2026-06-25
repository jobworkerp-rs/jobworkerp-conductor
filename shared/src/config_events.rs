use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// 設定変更イベント（Phase 0プロトタイプ版）
/// protobuf完全版はPhase 1で実装
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChangeEvent {
    pub event_type: String,
    pub name: String,
    pub timestamp: i64,
}

impl ConfigChangeEvent {
    /// Unix timestamp取得ヘルパー
    fn current_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// CronScheduler作成イベント（プロトタイプ版）
    pub fn cron_scheduler_created(name: String) -> Self {
        Self {
            event_type: "cron_scheduler_created".to_string(),
            name,
            timestamp: Self::current_timestamp(),
        }
    }

    /// CronScheduler更新イベント（プロトタイプ版）
    pub fn cron_scheduler_updated(name: String) -> Self {
        Self {
            event_type: "cron_scheduler_updated".to_string(),
            name,
            timestamp: Self::current_timestamp(),
        }
    }

    /// CronScheduler削除イベント（プロトタイプ版）
    pub fn cron_scheduler_deleted(name: String) -> Self {
        Self {
            event_type: "cron_scheduler_deleted".to_string(),
            name,
            timestamp: Self::current_timestamp(),
        }
    }

    /// テスト用イベント作成
    pub fn test_event(name: String) -> Self {
        Self {
            event_type: "test_event".to_string(),
            name,
            timestamp: Self::current_timestamp(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let event = ConfigChangeEvent::cron_scheduler_created("test_scheduler".to_string());
        assert_eq!(event.event_type, "cron_scheduler_created");
        assert_eq!(event.name, "test_scheduler");
        assert!(event.timestamp > 0);
    }
}
