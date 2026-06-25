/// Phase 1: protobuf完全版ConfigChangeEvent実装
use anyhow::Result;
use chrono::{DateTime, Utc};
use proto::jobworkerp_conductor::data::{
    config_change_event,
    ChangeAction,
    // event関連の構造体（統合済み）
    ConfigChangeEvent,
    CronSchedulerChangeEvent,
    CronSchedulerData,
    // 基本データ構造体
    CronSchedulerId,
    JobworkerpServer,
    JobworkerpServerChangeEvent,
    JobworkerpServerData,
    JobworkerpServerId,
    SlackEventHandlerChangeEvent,
    SlackEventHandlerData,
    SlackEventHandlerId,
    WorkerResultHandlerChangeEvent,
    WorkerResultHandlerData,
    WorkerResultHandlerId,
};
use std::collections::HashMap;

/// Entity ID型の型安全なenum
/// 各イベント型に対応する型付きIDを保持
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityId {
    CronScheduler(CronSchedulerId),
    WorkerResultHandler(WorkerResultHandlerId),
    JobworkerpServer(JobworkerpServerId),
    SlackEventHandler(SlackEventHandlerId),
}

impl EntityId {
    /// i64値を取得
    pub fn value(&self) -> i64 {
        match self {
            EntityId::CronScheduler(id) => id.value,
            EntityId::WorkerResultHandler(id) => id.value,
            EntityId::JobworkerpServer(id) => id.value,
            EntityId::SlackEventHandler(id) => id.value,
        }
    }
}

/// ConfigChangeEventのRustラッパー実装
/// protobuf生成コードに便利メソッドを追加
#[derive(Debug, Clone)]
pub struct ConfigChangeEventWrapper {
    pub inner: ConfigChangeEvent,
    pub timestamp: DateTime<Utc>,
}

impl ConfigChangeEventWrapper {
    /// 現在時刻でConfigChangeEventWrapperを作成
    pub fn new(event: ConfigChangeEvent) -> Self {
        Self {
            inner: event,
            timestamp: Utc::now(),
        }
    }

    /// CronScheduler作成イベントを作成
    pub fn create_cron_scheduler_created(
        name: String,
        id: Option<CronSchedulerId>,
        data: Option<CronSchedulerData>,
        jobworkerp_server: Option<JobworkerpServer>,
    ) -> Self {
        let event = CronSchedulerChangeEvent {
            action: ChangeAction::Created as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data,
            jobworkerp_server,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::CronScheduler(event)),
        };

        Self::new(config_event)
    }

    /// CronScheduler更新イベントを作成
    pub fn create_cron_scheduler_updated(
        name: String,
        id: Option<CronSchedulerId>,
        data: Option<CronSchedulerData>,
        jobworkerp_server: Option<JobworkerpServer>,
    ) -> Self {
        let event = CronSchedulerChangeEvent {
            action: ChangeAction::Updated as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data,
            jobworkerp_server,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::CronScheduler(event)),
        };

        Self::new(config_event)
    }

    /// CronScheduler削除イベントを作成
    pub fn create_cron_scheduler_deleted(name: String, id: Option<CronSchedulerId>) -> Self {
        let event = CronSchedulerChangeEvent {
            action: ChangeAction::Deleted as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data: None,
            jobworkerp_server: None,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::CronScheduler(event)),
        };

        Self::new(config_event)
    }

    /// WorkerResultHandler作成イベントを作成
    pub fn create_worker_result_handler_created(
        name: String,
        id: Option<WorkerResultHandlerId>,
        data: Option<WorkerResultHandlerData>,
        jobworkerp_servers: HashMap<i64, JobworkerpServer>,
    ) -> Self {
        let event = WorkerResultHandlerChangeEvent {
            action: ChangeAction::Created as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data,
            jobworkerp_servers,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::WorkerResultHandler(event)),
        };

        Self::new(config_event)
    }

    /// WorkerResultHandler更新イベントを作成
    pub fn create_worker_result_handler_updated(
        name: String,
        id: Option<WorkerResultHandlerId>,
        data: Option<WorkerResultHandlerData>,
        jobworkerp_servers: HashMap<i64, JobworkerpServer>,
    ) -> Self {
        let event = WorkerResultHandlerChangeEvent {
            action: ChangeAction::Updated as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data,
            jobworkerp_servers,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::WorkerResultHandler(event)),
        };

        Self::new(config_event)
    }

    /// WorkerResultHandler削除イベントを作成
    pub fn create_worker_result_handler_deleted(
        name: String,
        id: Option<WorkerResultHandlerId>,
        data: Option<WorkerResultHandlerData>,
        jobworkerp_servers: HashMap<i64, JobworkerpServer>,
    ) -> Self {
        let event = WorkerResultHandlerChangeEvent {
            action: ChangeAction::Deleted as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data,
            jobworkerp_servers,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::WorkerResultHandler(event)),
        };

        Self::new(config_event)
    }

    /// JobworkerpServer作成イベントを作成
    pub fn create_jobworkerp_server_created(
        name: String,
        id: JobworkerpServerId,
        data: Option<JobworkerpServerData>,
    ) -> Self {
        let event = JobworkerpServerChangeEvent {
            action: ChangeAction::Created as i32,
            name,
            id: Some(id),
            timestamp: Utc::now().timestamp(),
            data,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::JobworkerpServer(event)),
        };

        Self::new(config_event)
    }

    /// JobworkerpServer更新イベントを作成
    pub fn create_jobworkerp_server_updated(
        name: String,
        id: JobworkerpServerId,
        data: Option<JobworkerpServerData>,
    ) -> Self {
        let event = JobworkerpServerChangeEvent {
            action: ChangeAction::Updated as i32,
            name,
            id: Some(id),
            timestamp: Utc::now().timestamp(),
            data,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::JobworkerpServer(event)),
        };

        Self::new(config_event)
    }

    /// JobworkerpServer削除イベントを作成
    pub fn create_jobworkerp_server_deleted(
        name: String,
        id: JobworkerpServerId,
        data: Option<JobworkerpServerData>,
    ) -> Self {
        let event = JobworkerpServerChangeEvent {
            action: ChangeAction::Deleted as i32,
            name,
            id: Some(id),
            timestamp: Utc::now().timestamp(),
            data,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::JobworkerpServer(event)),
        };

        Self::new(config_event)
    }

    /// SlackEventHandler作成イベントを作成
    pub fn create_slack_event_handler_created(
        name: String,
        id: Option<SlackEventHandlerId>,
        data: Option<SlackEventHandlerData>,
        jobworkerp_server: Option<JobworkerpServer>,
    ) -> Self {
        let event = SlackEventHandlerChangeEvent {
            action: ChangeAction::Created as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data,
            jobworkerp_server,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::SlackEventHandler(event)),
        };

        Self::new(config_event)
    }

    /// SlackEventHandler更新イベントを作成
    pub fn create_slack_event_handler_updated(
        name: String,
        id: Option<SlackEventHandlerId>,
        data: Option<SlackEventHandlerData>,
        jobworkerp_server: Option<JobworkerpServer>,
    ) -> Self {
        let event = SlackEventHandlerChangeEvent {
            action: ChangeAction::Updated as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data,
            jobworkerp_server,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::SlackEventHandler(event)),
        };

        Self::new(config_event)
    }

    /// SlackEventHandler削除イベントを作成
    pub fn create_slack_event_handler_deleted(
        name: String,
        id: Option<SlackEventHandlerId>,
    ) -> Self {
        let event = SlackEventHandlerChangeEvent {
            action: ChangeAction::Deleted as i32,
            name,
            id,
            timestamp: Utc::now().timestamp(),
            data: None,
            jobworkerp_server: None,
        };

        let config_event = ConfigChangeEvent {
            event: Some(config_change_event::Event::SlackEventHandler(event)),
        };

        Self::new(config_event)
    }

    /// CronSchedulerイベントかどうか判定
    pub fn is_cron_scheduler(&self) -> bool {
        matches!(
            &self.inner.event,
            Some(config_change_event::Event::CronScheduler(_))
        )
    }

    /// WorkerResultHandlerイベントかどうか判定
    pub fn is_worker_result_handler(&self) -> bool {
        matches!(
            &self.inner.event,
            Some(config_change_event::Event::WorkerResultHandler(_))
        )
    }

    /// JobworkerpServerイベントかどうか判定
    pub fn is_jobworkerp_server(&self) -> bool {
        matches!(
            &self.inner.event,
            Some(config_change_event::Event::JobworkerpServer(_))
        )
    }

    /// SlackEventHandlerイベントかどうか判定
    pub fn is_slack_event_handler(&self) -> bool {
        matches!(
            &self.inner.event,
            Some(config_change_event::Event::SlackEventHandler(_))
        )
    }

    /// イベントが有効かどうか判定
    pub fn is_valid(&self) -> bool {
        self.inner.event.is_some()
    }

    /// CronSchedulerChangeEventを取得（存在する場合）
    pub fn as_cron_scheduler(&self) -> Option<&CronSchedulerChangeEvent> {
        match &self.inner.event {
            Some(config_change_event::Event::CronScheduler(event)) => Some(event),
            _ => None,
        }
    }

    /// WorkerResultHandlerChangeEventを取得（存在する場合）
    pub fn as_worker_result_handler(&self) -> Option<&WorkerResultHandlerChangeEvent> {
        match &self.inner.event {
            Some(config_change_event::Event::WorkerResultHandler(event)) => Some(event),
            _ => None,
        }
    }

    /// JobworkerpServerChangeEventを取得（存在する場合）
    pub fn as_jobworkerp_server(&self) -> Option<&JobworkerpServerChangeEvent> {
        match &self.inner.event {
            Some(config_change_event::Event::JobworkerpServer(event)) => Some(event),
            _ => None,
        }
    }

    /// SlackEventHandlerChangeEventを取得（存在する場合）
    pub fn as_slack_event_handler(&self) -> Option<&SlackEventHandlerChangeEvent> {
        match &self.inner.event {
            Some(config_change_event::Event::SlackEventHandler(event)) => Some(event),
            _ => None,
        }
    }

    /// エンティティ名を取得
    pub fn entity_name(&self) -> String {
        match &self.inner.event {
            Some(config_change_event::Event::CronScheduler(e)) => e.name.clone(),
            Some(config_change_event::Event::WorkerResultHandler(e)) => e.name.clone(),
            Some(config_change_event::Event::JobworkerpServer(e)) => e.name.clone(),
            Some(config_change_event::Event::SlackEventHandler(e)) => e.name.clone(),
            None => "unknown".to_string(),
        }
    }

    /// 型安全なエンティティIDを取得
    pub fn typed_id(&self) -> Option<EntityId> {
        match &self.inner.event {
            Some(config_change_event::Event::CronScheduler(e)) => e.id.map(EntityId::CronScheduler),
            Some(config_change_event::Event::WorkerResultHandler(e)) => {
                e.id.map(EntityId::WorkerResultHandler)
            }
            Some(config_change_event::Event::JobworkerpServer(e)) => {
                e.id.map(EntityId::JobworkerpServer)
            }
            Some(config_change_event::Event::SlackEventHandler(e)) => {
                e.id.map(EntityId::SlackEventHandler)
            }
            None => None,
        }
    }

    /// エンティティIDをi64で取得（Tested only）
    pub fn id(&self) -> Option<i64> {
        self.typed_id().map(|entity_id| entity_id.value())
    }

    /// アクション種別を取得
    pub fn action(&self) -> ChangeAction {
        match &self.inner.event {
            Some(config_change_event::Event::CronScheduler(e)) => e.action(),
            Some(config_change_event::Event::WorkerResultHandler(e)) => e.action(),
            Some(config_change_event::Event::JobworkerpServer(e)) => e.action(),
            Some(config_change_event::Event::SlackEventHandler(e)) => e.action(),
            None => ChangeAction::Unspecified,
        }
    }

    /// ラッパーレベルのタイムスタンプを秒単位で取得
    pub fn timestamp_secs(&self) -> i64 {
        self.timestamp.timestamp()
    }

    /// protobuf形式にシリアライズ
    pub fn to_protobuf_bytes(&self) -> Result<Vec<u8>> {
        use prost::Message;
        let mut buf = Vec::new();
        self.inner.encode(&mut buf)?;
        Ok(buf)
    }

    /// protobuf形式からデシリアライズ
    pub fn from_protobuf_bytes(bytes: &[u8]) -> Result<Self> {
        use prost::Message;
        let inner = ConfigChangeEvent::decode(bytes)?;
        Ok(Self::new(inner))
    }
}

/// ヘルパー関数
impl ConfigChangeEventWrapper {
    /// CronSchedulerIdを作成
    pub fn create_cron_scheduler_id(value: i64) -> CronSchedulerId {
        CronSchedulerId { value }
    }

    /// WorkerResultHandlerIdを作成
    pub fn create_worker_result_handler_id(value: i64) -> WorkerResultHandlerId {
        WorkerResultHandlerId { value }
    }

    /// JobworkerpServerIdを作成
    pub fn create_jobworkerp_server_id(value: i64) -> JobworkerpServerId {
        JobworkerpServerId { value }
    }

    /// テスト用のサンプルCronSchedulerDataを作成
    #[cfg(test)]
    pub fn create_sample_cron_scheduler_data() -> CronSchedulerData {
        use proto::jobworkerp_conductor::data::{
            cron_scheduler_data::ExecutionTarget, WorkflowExecution,
        };
        CronSchedulerData {
            name: "sample_scheduler".to_string(),
            jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
            workflow_url: "https://example.com/workflow.yml".to_string(),
            channel: Some("test_channel".to_string()),
            crontab: "0/5 * * * * *".to_string(),
            enabled: true,
            description: Some("Sample cron scheduler for testing".to_string()),
            created_at: Utc::now().timestamp(),
            updated_at: Utc::now().timestamp(),
            args: None,
            execution_target: Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "https://example.com/workflow.yml".to_string(),
                channel: Some("test_channel".to_string()),
            })),
        }
    }

    /// テスト用のイベント作成（レガシーメソッド互換性）
    pub fn test_event(name: String) -> Self {
        Self::create_cron_scheduler_created(name, None, None, None)
    }

    /// CronScheduler作成イベント作成（test only）
    pub fn test_cron_scheduler_created(name: String) -> Self {
        Self::create_cron_scheduler_created(name, None, None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_cron_scheduler_created_event() {
        let id = ConfigChangeEventWrapper::create_cron_scheduler_id(123);
        let data = ConfigChangeEventWrapper::create_sample_cron_scheduler_data();

        let event = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "test_scheduler".to_string(),
            Some(id),
            Some(data),
            None,
        );

        assert!(event.is_cron_scheduler());
        assert_eq!(event.entity_name(), "test_scheduler");
        assert_eq!(event.action(), ChangeAction::Created);
    }

    #[test]
    fn test_protobuf_serialization() {
        let event = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "serialize_test".to_string(),
            Some(ConfigChangeEventWrapper::create_cron_scheduler_id(456)),
            Some(ConfigChangeEventWrapper::create_sample_cron_scheduler_data()),
            None,
        );

        // シリアライズ
        let bytes = event.to_protobuf_bytes().unwrap();
        assert!(!bytes.is_empty());

        // デシリアライズ
        let deserialized = ConfigChangeEventWrapper::from_protobuf_bytes(&bytes).unwrap();
        assert!(deserialized.is_cron_scheduler());
        assert_eq!(deserialized.entity_name(), "serialize_test");
    }

    #[test]
    fn test_jobworkerp_server_event() {
        let id = ConfigChangeEventWrapper::create_jobworkerp_server_id(789);
        let data = JobworkerpServerData {
            name: "test_server".to_string(),
            host: "localhost".to_string(),
            port: "8080".to_string(),
            ssl_enabled: false,
            description: Some("Test server".to_string()),
            enabled: true,
            created_at: Utc::now().timestamp(),
            updated_at: Utc::now().timestamp(),
        };

        let event = ConfigChangeEventWrapper::create_jobworkerp_server_created(
            "test_server".to_string(),
            id,
            Some(data),
        );

        assert!(event.is_jobworkerp_server());
        assert_eq!(event.entity_name(), "test_server");
        assert_eq!(event.action(), ChangeAction::Created);
    }

    #[test]
    fn test_event_pattern_matching() {
        let cron_event = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "test_cron".to_string(),
            Some(ConfigChangeEventWrapper::create_cron_scheduler_id(1)),
            Some(ConfigChangeEventWrapper::create_sample_cron_scheduler_data()),
            None,
        );

        // oneofパターンマッチングの例
        if cron_event.is_cron_scheduler() {
            if let Some(cron_data) = cron_event.as_cron_scheduler() {
                assert_eq!(cron_data.name, "test_cron");
                assert_eq!(cron_data.action, ChangeAction::Created as i32);
            }
        }

        // 型安全な判定
        assert!(cron_event.is_cron_scheduler());
        assert!(!cron_event.is_worker_result_handler());
        assert!(!cron_event.is_jobworkerp_server());
        assert!(cron_event.is_valid());

        // ダイレクトアクセス
        let cron_inner = cron_event.as_cron_scheduler().unwrap();
        assert_eq!(cron_inner.name, "test_cron");
    }

    #[test]
    fn test_id_method() {
        // CronScheduler with id
        let event_with_id = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "test_scheduler".to_string(),
            Some(ConfigChangeEventWrapper::create_cron_scheduler_id(123)),
            Some(ConfigChangeEventWrapper::create_sample_cron_scheduler_data()),
            None,
        );
        assert_eq!(event_with_id.id(), Some(123));

        // CronScheduler without id
        let event_without_id = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "test_scheduler".to_string(),
            None,
            Some(ConfigChangeEventWrapper::create_sample_cron_scheduler_data()),
            None,
        );
        assert_eq!(event_without_id.id(), None);

        // WorkerResultHandler with id
        let handler_event = ConfigChangeEventWrapper::create_worker_result_handler_created(
            "test_handler".to_string(),
            Some(ConfigChangeEventWrapper::create_worker_result_handler_id(
                456,
            )),
            None,
            std::collections::HashMap::new(),
        );
        assert_eq!(handler_event.id(), Some(456));

        // JobworkerpServer with id
        let server_event = ConfigChangeEventWrapper::create_jobworkerp_server_created(
            "test_server".to_string(),
            ConfigChangeEventWrapper::create_jobworkerp_server_id(789),
            None,
        );
        assert_eq!(server_event.id(), Some(789));
    }

    #[test]
    fn test_event_handling_pattern() {
        let events = vec![
            ConfigChangeEventWrapper::create_cron_scheduler_created(
                "scheduler1".to_string(),
                Some(ConfigChangeEventWrapper::create_cron_scheduler_id(1)),
                None,
                None,
            ),
            ConfigChangeEventWrapper::create_jobworkerp_server_created(
                "server1".to_string(),
                ConfigChangeEventWrapper::create_jobworkerp_server_id(2),
                None,
            ),
        ];

        for event in &events {
            match () {
                _ if event.is_cron_scheduler() => {
                    let cron_event = event.as_cron_scheduler().unwrap();
                    println!("Processing cron scheduler: {}", cron_event.name);
                }
                _ if event.is_jobworkerp_server() => {
                    let server_event = event.as_jobworkerp_server().unwrap();
                    println!("Processing jobworkerp server: {}", server_event.name);
                }
                _ if event.is_worker_result_handler() => {
                    let handler_event = event.as_worker_result_handler().unwrap();
                    println!("Processing worker result handler: {}", handler_event.name);
                }
                _ => {
                    println!("Unknown event type");
                }
            }
        }
    }

    #[test]
    fn test_typed_id() {
        // CronScheduler with typed_id
        let cron_event = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "test_scheduler".to_string(),
            Some(ConfigChangeEventWrapper::create_cron_scheduler_id(123)),
            Some(ConfigChangeEventWrapper::create_sample_cron_scheduler_data()),
            None,
        );

        match cron_event.typed_id() {
            Some(EntityId::CronScheduler(id)) => {
                assert_eq!(id.value, 123);
            }
            _ => panic!("Expected CronScheduler ID"),
        }

        // WorkerResultHandler with typed_id
        let handler_event = ConfigChangeEventWrapper::create_worker_result_handler_created(
            "test_handler".to_string(),
            Some(ConfigChangeEventWrapper::create_worker_result_handler_id(
                456,
            )),
            None,
            std::collections::HashMap::new(),
        );

        match handler_event.typed_id() {
            Some(EntityId::WorkerResultHandler(id)) => {
                assert_eq!(id.value, 456);
            }
            _ => panic!("Expected WorkerResultHandler ID"),
        }

        // JobworkerpServer with typed_id
        let server_event = ConfigChangeEventWrapper::create_jobworkerp_server_created(
            "test_server".to_string(),
            ConfigChangeEventWrapper::create_jobworkerp_server_id(789),
            None,
        );

        match server_event.typed_id() {
            Some(EntityId::JobworkerpServer(id)) => {
                assert_eq!(id.value, 789);
            }
            _ => panic!("Expected JobworkerpServer ID"),
        }

        // Event without id
        let event_without_id = ConfigChangeEventWrapper::create_cron_scheduler_created(
            "test".to_string(),
            None,
            None,
            None,
        );
        assert_eq!(event_without_id.typed_id(), None);
    }
}
