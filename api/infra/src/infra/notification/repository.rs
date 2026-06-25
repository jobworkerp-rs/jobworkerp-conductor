use shared::notification::{NotificationReceiver, NotificationRepository};
pub use shared::notification::{
    NotificationReceiver as NotificationReceiverTrait,
    NotificationRepository as NotificationRepositoryTrait,
};

/// NotificationRepository実装のタイプエイリアス
///
/// 具体的な実装（Channel, Redis）を抽象化して扱うためのエイリアス。
pub type DynNotificationRepository = dyn NotificationRepository + Send + Sync;
pub type DynNotificationReceiver = dyn NotificationReceiver + Send + Sync;
