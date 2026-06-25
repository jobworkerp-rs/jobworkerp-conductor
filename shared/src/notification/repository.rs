use crate::config_events_proto::ConfigChangeEventWrapper as ConfigChangeEvent;
use anyhow::Result;
use async_trait::async_trait;

/// 通知リポジトリ抽象化トレイト
///
/// インフラ層での通知システム実装（Channel, Redis等）の抽象化。
/// クリーンアーキテクチャの依存関係逆転原則に従い、
/// 下位レイヤー（infra）が上位レイヤー（shared）のtraitを実装する。
#[async_trait]
pub trait NotificationRepository: Send + Sync {
    /// 設定変更イベントを発行
    async fn publish(&self, event: ConfigChangeEvent) -> Result<()>;

    /// 通知受信者を作成
    async fn subscribe(&self) -> Result<Box<dyn NotificationReceiver>>;
}

/// 通知受信者抽象化トレイト
///
/// 通知イベントの受信処理を抽象化。
/// 異なる実装（Channel, Redis等）に対して統一インターフェースを提供。
#[async_trait]
pub trait NotificationReceiver: Send + Sync {
    /// 通知イベントを受信
    ///
    /// Returns:
    /// - Ok(Some(event)): イベント受信成功
    /// - Ok(None): 受信チャンネルクローズ（正常終了）
    /// - Err(e): 受信エラー
    async fn receive(&mut self) -> Result<Option<ConfigChangeEvent>>;
}
