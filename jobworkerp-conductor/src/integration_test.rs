use crate::dynamic::scheduler_manager::DynamicSchedulerManager;
use anyhow::Result;
use shared::config_events_proto::ConfigChangeEventWrapper as ConfigChangeEvent;
use shared::notification::{ConfigChangeNotificationService, MemoryNotificationService};
use shared::{LocalConfigStore, SharedLocalConfigStore};
use std::sync::{Arc, RwLock};
use tokio::time::{timeout, Duration};

#[cfg(test)]
mod tests {
    use super::*;

    /// スケジューラーと通知システムの統合テスト
    pub struct IntegratedSchedulerService {
        scheduler_manager: DynamicSchedulerManager,
        notification_service: MemoryNotificationService,
    }

    impl IntegratedSchedulerService {
        /// 新しい統合サービスを作成
        pub async fn new() -> Result<Self> {
            let scheduler_manager = DynamicSchedulerManager::new().await?;
            let notification_service = MemoryNotificationService::new_default();

            Ok(Self {
                scheduler_manager,
                notification_service,
            })
        }

        /// スケジューラー開始（通知付き）
        #[cfg(test)]
        pub async fn test_start(&mut self) -> Result<()> {
            self.scheduler_manager.start().await?;

            // サービス開始通知
            let event = ConfigChangeEvent::create_cron_scheduler_created(
                "scheduler_service_started".to_string(),
                None,
                None,
                None,
            );
            self.notification_service.notify(event).await?;

            Ok(())
        }

        /// 通知システムにサブスクライブ
        pub async fn subscribe(
            &self,
        ) -> Result<Box<dyn shared::notification::ConfigChangeEventReceiver>> {
            self.notification_service.subscribe().await
        }

        /// ジョブ追加（通知付き）
        pub async fn add_job_with_notification(
            &mut self,
            id: i64,
            name: String,
            cron_expr: String,
        ) -> Result<()> {
            // ジョブ追加前通知
            let before_event = ConfigChangeEvent::test_cron_scheduler_created(name.clone());
            self.notification_service.notify(before_event).await?;

            // ジョブ追加
            self.scheduler_manager
                .add_test_job(id, name.clone(), cron_expr)
                .await?;

            // ジョブ追加後通知
            let after_event = ConfigChangeEvent::create_cron_scheduler_created(
                format!("job_added_{name}"),
                None,
                None,
                None,
            );
            self.notification_service.notify(after_event).await?;

            Ok(())
        }

        /// 安全なリロード（通知付き）
        pub async fn safe_reload_with_notification(
            &mut self,
            test_jobs: Vec<(i64, String, String)>,
        ) -> Result<()> {
            // リロード開始通知
            let start_event = ConfigChangeEvent::create_cron_scheduler_created(
                "reload_started".to_string(),
                None,
                None,
                None,
            );
            self.notification_service.notify(start_event).await?;

            // 安全なリロード実行（テスト用メソッドを使用）
            self.scheduler_manager.safe_reload_test(test_jobs).await?;

            // リロード完了通知
            let complete_event = ConfigChangeEvent::create_cron_scheduler_created(
                "reload_completed".to_string(),
                None,
                None,
                None,
            );
            self.notification_service.notify(complete_event).await?;

            Ok(())
        }

        /// アクティブジョブ数取得
        pub fn active_job_count(&self) -> usize {
            self.scheduler_manager.active_job_count()
        }

        /// 通知サービス統計
        pub fn notification_stats(&self) -> (usize, usize) {
            (
                self.notification_service.receiver_count(),
                self.notification_service.len(),
            )
        }
    }

    #[tokio::test]
    async fn test_integrated_service_basic() -> Result<()> {
        let mut service = IntegratedSchedulerService::new().await?;
        let mut receiver = service.subscribe().await?;

        // サービス開始
        service.test_start().await?;

        // 開始通知を受信
        let start_event = timeout(Duration::from_secs(1), receiver.receive()).await??;
        assert!(start_event.is_some());
        let event = start_event.unwrap();
        assert_eq!(event.entity_name(), "scheduler_service_started");

        // 統計確認
        let (receiver_count, queue_len) = service.notification_stats();
        assert_eq!(receiver_count, 1);
        assert_eq!(queue_len, 0); // イベントは受信済み

        Ok(())
    }

    #[tokio::test]
    async fn test_job_lifecycle_with_notifications() -> Result<()> {
        let mut service = IntegratedSchedulerService::new().await?;
        service.test_start().await?;

        let mut receiver = service.subscribe().await?;

        // ジョブ追加（通知付き）
        service
            .add_job_with_notification(1, "test_job".to_string(), "0/5 * * * * *".to_string())
            .await?;

        // スケジューラー状態確認
        assert_eq!(service.active_job_count(), 1);

        // 通知イベント確認（2つのイベントが送信される）
        let event1 = timeout(Duration::from_secs(1), receiver.receive()).await??;
        assert!(event1.is_some());
        let event1 = event1.unwrap();
        assert_eq!(
            event1.action(),
            proto::jobworkerp_conductor::data::ChangeAction::Created
        );
        assert_eq!(event1.entity_name(), "test_job");

        let event2 = timeout(Duration::from_secs(1), receiver.receive()).await??;
        assert!(event2.is_some());
        let event2 = event2.unwrap();
        assert_eq!(
            event2.action(),
            proto::jobworkerp_conductor::data::ChangeAction::Created
        );
        assert_eq!(event2.entity_name(), "job_added_test_job");

        Ok(())
    }

    #[tokio::test]
    async fn test_safe_reload_with_notifications() -> Result<()> {
        let mut service = IntegratedSchedulerService::new().await?;
        service.test_start().await?;

        let mut receiver = service.subscribe().await?;

        // 初期ジョブ追加
        service
            .add_job_with_notification(1, "initial_job".to_string(), "0/10 * * * * *".to_string())
            .await?;
        assert_eq!(service.active_job_count(), 1);

        // 既存通知をクリア（3つのイベント: start + create + added）
        for _ in 0..3 {
            let _ = timeout(Duration::from_millis(100), receiver.receive()).await;
        }

        // 安全なリロード（通知付き）
        let new_jobs = vec![
            (2, "job1".to_string(), "0/3 * * * * *".to_string()),
            (3, "job2".to_string(), "0/7 * * * * *".to_string()),
        ];

        service.safe_reload_with_notification(new_jobs).await?;
        assert_eq!(service.active_job_count(), 2);

        // リロード通知確認
        let start_event = timeout(Duration::from_secs(1), receiver.receive()).await??;
        assert!(start_event.is_some());
        let event = start_event.unwrap();
        assert_eq!(event.entity_name(), "reload_started");

        let complete_event = timeout(Duration::from_secs(1), receiver.receive()).await??;
        assert!(complete_event.is_some());
        let event = complete_event.unwrap();
        assert_eq!(event.entity_name(), "reload_completed");

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_subscribers() -> Result<()> {
        let mut service = IntegratedSchedulerService::new().await?;
        service.test_start().await?;

        // 複数の受信者を作成
        let mut receiver1 = service.subscribe().await?;
        let mut receiver2 = service.subscribe().await?;
        let mut receiver3 = service.subscribe().await?;

        let (receiver_count, _) = service.notification_stats();
        assert_eq!(receiver_count, 3);

        // ジョブ追加
        service
            .add_job_with_notification(
                1,
                "broadcast_test".to_string(),
                "0/15 * * * * *".to_string(),
            )
            .await?;

        // 全受信者がイベントを受信（最初のイベントのみテスト）
        let event1 = timeout(Duration::from_secs(1), receiver1.receive()).await??;
        let event2 = timeout(Duration::from_secs(1), receiver2.receive()).await??;
        let event3 = timeout(Duration::from_secs(1), receiver3.receive()).await??;

        assert!(event1.is_some());
        assert!(event2.is_some());
        assert!(event3.is_some());

        let e1 = event1.unwrap();
        let e2 = event2.unwrap();
        let e3 = event3.unwrap();

        assert_eq!(e1.entity_name(), "broadcast_test");
        assert_eq!(e2.entity_name(), "broadcast_test");
        assert_eq!(e3.entity_name(), "broadcast_test");

        Ok(())
    }

    // #[tokio::test]
    // async fn test_initialization_layer_integration() -> Result<()> {
    //     use crate::initialization::InitializationAppModule;

    //     // MockAppModule for integration testing
    //     struct IntegrationMockAppModule;

    //     #[async_trait::async_trait]
    //     impl InitializationAppModule for IntegrationMockAppModule {
    //         async fn load_all_cron_schedulers(
    //             &self,
    //         ) -> Result<Vec<proto::jobworkerp_conductor::data::CronScheduler>> {
    //             Ok(Vec::new())
    //         }

    //         async fn load_all_worker_result_handlers(
    //             &self,
    //         ) -> Result<Vec<proto::jobworkerp_conductor::data::WorkerResultHandler>> {
    //             Ok(Vec::new())
    //         }

    //         async fn load_all_jobworkerp_servers(
    //             &self,
    //         ) -> Result<Vec<proto::jobworkerp_conductor::data::JobworkerpServer>> {
    //             Ok(Vec::new())
    //         }
    //     }

    //     // InitializationLayerの作成
    //     let app_modules = std::sync::Arc::new(IntegrationMockAppModule);
    //     let initialization_layer = InitializationLayer::new(app_modules);

    //     // 初期設定の読み込み
    //     let initial_config = initialization_layer.load_initial_config().await?;
    //     assert_eq!(initial_config.total_count(), 0); // プロトタイプ実装では空

    //     // LocalConfigStoreの作成と初期化
    //     let local_config_store = LocalConfigStore::from_initial_config(initial_config.clone());
    //     let stats = local_config_store.get_stats();
    //     assert_eq!(stats.total_count(), 0);

    //     // 通知サービスの作成
    //     let notification_service =
    //         Arc::new(ConfigChangeNotificationServiceImpl::new_memory_default().unwrap());

    //     // EventHandlerServerManagerの作成（プレースホルダー）
    //     let _server_manager = initialization_layer
    //         .create_server_manager(initial_config, notification_service.clone())?;

    //     Ok(())
    // }

    #[tokio::test]
    async fn test_local_config_store_operations() -> Result<()> {
        let shared_store: SharedLocalConfigStore = Arc::new(RwLock::new(LocalConfigStore::new()));

        // 読み取りアクセステスト
        {
            let reader = shared_store.read().unwrap();
            let stats = reader.get_stats();
            assert_eq!(stats.total_count(), 0);
        }

        // 書き込みアクセステスト
        {
            let mut writer = shared_store.write().unwrap();
            writer.update_sync_time();
            let stats = writer.get_stats();
            assert_eq!(stats.total_count(), 0);
        }

        // 整合性チェック
        {
            let reader = shared_store.read().unwrap();
            assert!(reader.validate_consistency().is_ok());
        }

        Ok(())
    }

    // #[tokio::test]
    // async fn test_notification_service_builder() -> Result<()> {
    //     // ビルダーパターンでのサービス作成
    //     let service = NotificationServiceBuilder::new()
    //         .with_memory_backend()
    //         .with_capacity(2000)
    //         .build()?;

    //     // サービス機能テスト
    //     let legacy_event = shared::config_events::ConfigChangeEvent::create_cron_scheduler_created(
    //         "builder_integration_test".to_string(, None, None, None),
    //     );
    //     let event = ConfigChangeEvent::from_legacy(legacy_event);
    //     service.notify(event).await?;

    //     // 受信者作成・テスト
    //     let mut receiver = service.subscribe().await?;

    //     let legacy_test_event = shared::config_events::ConfigChangeEvent::cron_scheduler_created(
    //         "integration_scheduler".to_string(),
    //     );
    //     let test_event = ConfigChangeEvent::from_legacy(legacy_test_event);
    //     let notify_service = service.clone(); // Arc<dyn Trait> のクローン

    //     tokio::spawn(async move {
    //         tokio::time::sleep(Duration::from_millis(50)).await;
    //         let _ = notify_service.notify(test_event).await;
    //     });

    //     let received = timeout(Duration::from_secs(1), receiver.receive()).await??;
    //     assert!(received.is_some());
    //     let event = received.unwrap();
    //     assert_eq!(event.entity_name(), "integration_scheduler");

    //     Ok(())
    // }

    //     #[tokio::test]
    //     async fn test_complete_initialization_flow() -> Result<()> {
    //         use crate::initialization::InitializationAppModule;

    //         // MockAppModule for complete flow testing
    //         struct CompleteMockAppModule;

    //         #[async_trait::async_trait]
    //         impl InitializationAppModule for CompleteMockAppModule {
    //             async fn load_all_cron_schedulers(
    //                 &self,
    //             ) -> Result<Vec<proto::jobworkerp_conductor::data::CronScheduler>> {
    //                 Ok(Vec::new())
    //             }

    //             async fn load_all_worker_result_handlers(
    //                 &self,
    //             ) -> Result<Vec<proto::jobworkerp_conductor::data::WorkerResultHandler>> {
    //                 Ok(Vec::new())
    //             }

    //             async fn load_all_jobworkerp_servers(
    //                 &self,
    //             ) -> Result<Vec<proto::jobworkerp_conductor::data::JobworkerpServer>> {
    //                 Ok(Vec::new())
    //             }
    //         }

    //         // 1. 通知サービス作成
    //         let notification_service = Arc::new(
    //             NotificationServiceBuilder::new()
    //                 .with_memory_backend()
    //                 .build()?,
    //         );

    //         // 2. InitializationLayer作成
    //         let app_modules = std::sync::Arc::new(CompleteMockAppModule);
    //         let initialization_layer = InitializationLayer::new(app_modules);

    //         // 3. 初期設定読み込み
    //         let initial_config = initialization_layer.load_initial_config().await?;

    //         // 4. LocalConfigStore作成
    //         let local_store = Arc::new(RwLock::new(LocalConfigStore::from_initial_config(
    //             initial_config.clone(),
    //         )));

    //         // 5. EventHandlerServerManager作成（プレースホルダー）
    //         let _server_manager = initialization_layer
    //             .create_server_manager(initial_config, notification_service.clone())?;

    //         // 6. 初期化完了を通知
    //         let legacy_completion_event = shared::config_events::ConfigChangeEvent::create_cron_scheduler_created(
    //             "initialization_completed".to_string(, None, None, None),
    //         );
    //         let completion_event = ConfigChangeEvent::from_legacy(legacy_completion_event);
    //         notification_service.notify(completion_event).await?;

    //         // 7. LocalConfigStore状態確認
    //         {
    //             let reader = local_store.read().unwrap();
    //             assert!(reader.validate_consistency().is_ok());
    //             let stats = reader.get_stats();
    //             assert_eq!(stats.total_count(), 0); // プロトタイプ実装では空
    //         }

    //         tracing::info!("Complete initialization flow test completed successfully");
    //         Ok(())
    //     }
}
