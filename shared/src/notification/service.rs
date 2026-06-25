use crate::config_events_proto::ConfigChangeEventWrapper as ConfigChangeEvent;
use anyhow::Result;
use async_trait::async_trait;

/// 設定変更通知サービス抽象化トレイト
///
/// アプリケーション層での通知サービスの抽象化。
/// UI Event Handler層が使用する統一インターフェース。
#[async_trait]
pub trait ConfigChangeNotificationService: Send + Sync {
    /// 設定変更イベントを通知
    async fn notify(&self, event: ConfigChangeEvent) -> Result<()>;

    /// 設定変更イベント受信者を作成
    async fn subscribe(&self) -> Result<Box<dyn ConfigChangeEventReceiver>>;
}

/// 設定変更イベント受信者抽象化トレイト
///
/// UI Event Handler層での設定変更イベント受信処理を抽象化。
/// ビジネスロジック層での統一インターフェース。
#[async_trait]
pub trait ConfigChangeEventReceiver: Send + Sync {
    /// 設定変更イベントを受信
    ///
    /// Returns:
    /// - Ok(Some(event)): イベント受信成功
    /// - Ok(None): 受信チャンネルクローズ（正常終了）
    /// - Err(e): 受信エラー
    async fn receive(&mut self) -> Result<Option<ConfigChangeEvent>>;
}
