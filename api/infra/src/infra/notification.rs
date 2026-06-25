//! インフラ層通知システム実装
//!
//! shared層の抽象化traitを実装し、具体的な通知メカニズム（Channel, Redis）を提供。
//! クリーンアーキテクチャの依存関係逆転原則に従った実装。

pub mod channel;
pub mod factory;
pub mod redis;
pub mod repository;

// 公開API
pub use channel::ChannelNotificationRepository;
pub use factory::{NotificationConfig, NotificationRepositoryFactory};
pub use redis::RedisNotificationRepository;
pub use repository::{DynNotificationReceiver, DynNotificationRepository};
