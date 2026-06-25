use anyhow::Result;
use proto::jobworkerp_conductor::data::{
    CronScheduler, JobworkerpServer, SlackEventHandler, WorkerResultHandler,
};
use shared::initialization::InitializationConfigLoader;
use shared::notification::ConfigChangeNotificationService;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct InitialConfig {
    pub cron_schedulers: Vec<CronScheduler>,
    pub worker_result_handlers: Vec<WorkerResultHandler>,
    pub jobworkerp_servers: Vec<JobworkerpServer>,
    pub slack_event_handlers: Vec<SlackEventHandler>,
}

impl InitialConfig {
    pub fn new(
        cron_schedulers: Vec<CronScheduler>,
        worker_result_handlers: Vec<WorkerResultHandler>,
        jobworkerp_servers: Vec<JobworkerpServer>,
        slack_event_handlers: Vec<SlackEventHandler>,
    ) -> Self {
        Self {
            cron_schedulers,
            worker_result_handlers,
            jobworkerp_servers,
            slack_event_handlers,
        }
    }

    pub fn empty() -> Self {
        Self {
            cron_schedulers: Vec::new(),
            worker_result_handlers: Vec::new(),
            jobworkerp_servers: Vec::new(),
            slack_event_handlers: Vec::new(),
        }
    }

    pub fn total_count(&self) -> usize {
        self.cron_schedulers.len()
            + self.worker_result_handlers.len()
            + self.jobworkerp_servers.len()
            + self.slack_event_handlers.len()
    }

    pub fn cron_scheduler_count(&self) -> usize {
        self.cron_schedulers.len()
    }

    pub fn worker_result_handler_count(&self) -> usize {
        self.worker_result_handlers.len()
    }

    pub fn jobworkerp_server_count(&self) -> usize {
        self.jobworkerp_servers.len()
    }

    pub fn slack_event_handler_count(&self) -> usize {
        self.slack_event_handlers.len()
    }
}

/// InitializationLayer - 循環参照回避の核心コンポーネント
///
/// App層への依存を初期化時のみに限定し、EventHandlerServerManagerを
/// 完全に独立させるための設定事前読み込み層
pub struct InitializationLayer<T> {
    app_modules: Arc<T>,
}

impl<T> InitializationLayer<T>
where
    T: InitializationConfigLoader + Send + Sync + 'static,
{
    pub fn new(app_modules: Arc<T>) -> Self {
        Self { app_modules }
    }

    /// App層に依存するメソッドはここに集約
    /// これにより、EventHandlerServerManagerはApp層への直接依存を持たない
    pub async fn load_initial_config(&self) -> Result<InitialConfig> {
        tracing::info!("Starting initial configuration loading from database");

        // 各App層から設定を読み込み
        let (cron_schedulers, worker_result_handlers, jobworkerp_servers, slack_event_handlers) =
            self.load_all_configs().await?;

        let initial_config = InitialConfig::new(
            cron_schedulers,
            worker_result_handlers,
            jobworkerp_servers,
            slack_event_handlers,
        );

        tracing::info!(
            "Initial configuration loaded: {} cron_schedulers, {} worker_result_handlers, {} jobworkerp_servers, {} slack_event_handlers (total: {})",
            initial_config.cron_scheduler_count(),
            initial_config.worker_result_handler_count(),
            initial_config.jobworkerp_server_count(),
            initial_config.slack_event_handler_count(),
            initial_config.total_count()
        );

        Ok(initial_config)
    }

    /// 各設定の並行読み込み（効率化）
    /// App層の各サービスから設定を並行取得
    async fn load_all_configs(
        &self,
    ) -> Result<(
        Vec<CronScheduler>,
        Vec<WorkerResultHandler>,
        Vec<JobworkerpServer>,
        Vec<SlackEventHandler>,
    )> {
        tracing::debug!("Loading configurations from App layer in parallel");

        // 並行実行で効率化
        let (
            cron_schedulers_result,
            worker_result_handlers_result,
            jobworkerp_servers_result,
            slack_event_handlers_result,
        ) = tokio::try_join!(
            self.app_modules.load_all_cron_schedulers(),
            self.app_modules.load_all_worker_result_handlers(),
            self.app_modules.load_all_jobworkerp_servers(),
            self.app_modules.load_all_slack_event_handlers()
        )?;

        tracing::debug!(
            "Loaded {} cron_schedulers, {} worker_result_handlers, {} jobworkerp_servers, {} slack_event_handlers from database",
            cron_schedulers_result.len(),
            worker_result_handlers_result.len(),
            jobworkerp_servers_result.len(),
            slack_event_handlers_result.len()
        );

        Ok((
            cron_schedulers_result,
            worker_result_handlers_result,
            jobworkerp_servers_result,
            slack_event_handlers_result,
        ))
    }

    pub fn create_server_manager(
        &self,
        initial_config: InitialConfig,
        notification_service: Arc<dyn ConfigChangeNotificationService>,
        execution_ref_recorder: shared::SharedExecutionRefRecorder,
    ) -> Result<EventHandlerServerManager> {
        tracing::info!("Creating EventHandlerServerManager with initial config");

        let server_manager = EventHandlerServerManager::new_with_initial_config(
            initial_config,
            notification_service,
            execution_ref_recorder,
        )?;

        tracing::info!("EventHandlerServerManager created successfully");
        Ok(server_manager)
    }
}

/// EventHandlerServerManager - UI Event Handler層の中核管理コンポーネント
///
/// 循環参照を完全に回避し、App層への直接依存を持たない設計。
/// 初期化時にInitializationLayerからデータを受け取り、以降は完全に独立動作。
pub struct EventHandlerServerManager {
    // ローカル設定管理（循環参照回避の核心）
    local_config_store: shared::SharedLocalConfigStore,

    // 動的管理コンポーネント
    scheduler_manager: Option<Arc<crate::dynamic::scheduler_manager::DynamicSchedulerManager>>,
    listener_manager: crate::dynamic::listener_manager::DynamicListenerManager,
    slack_handler_manager:
        Option<Arc<tokio::sync::Mutex<slack_event_handler::DynamicSlackHandlerManager>>>,

    // 通知受信のみ（送信はApp層が担当）
    notification_service: Arc<dyn ConfigChangeNotificationService>,
    execution_ref_recorder: shared::SharedExecutionRefRecorder,

    // 稼働状態管理
    is_running: Arc<std::sync::atomic::AtomicBool>,
}

impl EventHandlerServerManager {
    pub fn new_with_initial_config(
        initial_config: InitialConfig,
        notification_service: Arc<dyn ConfigChangeNotificationService>,
        execution_ref_recorder: shared::SharedExecutionRefRecorder,
    ) -> Result<Self> {
        tracing::info!(
            "Creating EventHandlerServerManager with {} initial configs",
            initial_config.total_count()
        );

        // LocalConfigStoreに初期設定をロード
        use crate::dynamic::local_config::LocalConfigStoreExt;
        let local_config_store = Arc::new(std::sync::RwLock::new(
            shared::LocalConfigStore::from_initial_config(initial_config),
        ));

        // DynamicSchedulerManagerを初期化（Phase 0で実装済み）
        // Note: 非同期初期化のため、start()メソッドで実際に初期化する
        // プレースホルダーとしてNoneを設定
        let scheduler_manager: Option<
            Arc<crate::dynamic::scheduler_manager::DynamicSchedulerManager>,
        > = None;

        // DynamicListenerManager を初期化（Phase 3統合）
        let listener_manager =
            crate::dynamic::listener_manager::DynamicListenerManager::new_with_local_config_and_recorder(
                local_config_store.clone(),
                execution_ref_recorder.clone(),
            );

        // DynamicSlackHandlerManager を初期化（Phase 4統合）
        let slack_handler_manager = Some(Arc::new(tokio::sync::Mutex::new(
            slack_event_handler::DynamicSlackHandlerManager::new_with_recorder(
                local_config_store.clone(),
                execution_ref_recorder.clone(),
            ),
        )));

        let server_manager = Self {
            local_config_store,
            scheduler_manager,
            listener_manager,
            slack_handler_manager,
            notification_service,
            execution_ref_recorder,
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        tracing::info!("EventHandlerServerManager created successfully");
        Ok(server_manager)
    }

    /// サーバー開始
    pub async fn start(&mut self) -> Result<()> {
        // 原子的にfalse→trueに変更（排他制御）
        if self
            .is_running
            .compare_exchange(
                false,                                // expected
                true,                                 // new
                std::sync::atomic::Ordering::Acquire, // success ordering
                std::sync::atomic::Ordering::Relaxed, // failure ordering
            )
            .is_err()
        {
            return Err(anyhow::anyhow!(
                "EventHandlerServerManager is already running"
            ));
        }

        // この時点で確実に1つのスレッドだけが通過
        tracing::info!("Starting EventHandlerServerManager");

        // エラー時のrollback処理
        let result = async {
            // 初期スナップショット作成
            {
                let mut store = self
                    .local_config_store
                    .write()
                    .map_err(|e| anyhow::anyhow!("Failed to write lock config store: {}", e))?;
                store.create_snapshot("Initial startup snapshot".to_string());
            }

            // 初期化処理
            self.start_initial_listeners().await?;

            if self.scheduler_manager.is_none() {
                self.scheduler_manager = Some(Arc::new(
                    crate::dynamic::scheduler_manager::DynamicSchedulerManager::new_with_local_config(
                        self.local_config_store.clone(),
                        self.execution_ref_recorder.clone(),
                    )
                    .await,
                ));
            }

            self.start_initial_schedulers().await?;
            self.start_slack_handler().await?;
            self.start_notification_listener().await?;

            Ok(())
        }.await;

        // エラー時はis_runningをfalseに戻す
        if result.is_err() {
            self.is_running
                .store(false, std::sync::atomic::Ordering::Release);
            return result;
        }

        tracing::info!("EventHandlerServerManager started successfully");
        Ok(())
    }

    /// サーバー停止
    pub async fn stop(&mut self) -> Result<()> {
        if !self.is_running.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }

        tracing::info!("Stopping EventHandlerServerManager");

        // 全リスナーを停止
        self.listener_manager.stop_all().await?;

        // 停止前スナップショットを作成
        {
            let mut store = self
                .local_config_store
                .write()
                .map_err(|e| anyhow::anyhow!("Failed to write lock config store: {}", e))?;
            store.create_snapshot("Before shutdown snapshot".to_string());
        }

        self.is_running
            .store(false, std::sync::atomic::Ordering::Release);
        tracing::info!("EventHandlerServerManager stopped");

        Ok(())
    }

    /// 稼働状態確認
    pub fn is_running(&self) -> bool {
        self.is_running.load(std::sync::atomic::Ordering::Acquire)
    }

    /// ローカル設定統計取得
    pub fn get_config_stats(&self) -> Result<shared::LocalConfigStats> {
        let store = self
            .local_config_store
            .read()
            .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;
        Ok(store.get_stats())
    }

    /// アクティブリスナー統計取得（Phase 3統合）
    pub fn get_active_listener_count(&self) -> usize {
        self.listener_manager.active_listener_count()
    }

    /// 初期設定からリスナーを起動
    async fn start_initial_listeners(&mut self) -> Result<()> {
        let (handlers, enabled_count) = {
            let store = self
                .local_config_store
                .read()
                .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;

            let handlers = store
                .get_all_worker_result_handlers()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let enabled_count = handlers
                .iter()
                .filter(|h| h.data.as_ref().is_some_and(|d| d.enabled))
                .count();
            (handlers, enabled_count)
        };

        tracing::info!(
            "Starting {} enabled worker result handlers from initial config",
            enabled_count
        );

        for handler in handlers {
            if let (Some(id), Some(data)) = (&handler.id, &handler.data) {
                if data.enabled {
                    match self.listener_manager.add_listener_from_local(id).await {
                        Ok(_) => {
                            tracing::info!(
                                "Successfully started listener: id={}, name={}",
                                id.value,
                                data.name
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to start listener id={}, name='{}': {}. Continuing with other listeners.",
                                id.value,
                                data.name,
                                e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 初期設定からスケジューラーを起動
    async fn start_initial_schedulers(&mut self) -> Result<()> {
        if let Some(scheduler_manager) = &self.scheduler_manager {
            let (schedulers, enabled_count) = {
                let store = self
                    .local_config_store
                    .read()
                    .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;

                let schedulers = store
                    .get_all_cron_schedulers()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let enabled_count = schedulers
                    .iter()
                    .filter(|s| s.data.as_ref().is_some_and(|d| d.enabled))
                    .count();
                (schedulers, enabled_count)
            };

            tracing::info!(
                "Starting {} enabled cron schedulers from initial config",
                enabled_count
            );

            for scheduler in schedulers {
                if let (Some(id), Some(data)) = (&scheduler.id, &scheduler.data) {
                    if data.enabled {
                        match scheduler_manager.add_scheduler_from_local(id).await {
                            Ok(_) => {
                                tracing::info!(
                                    "Successfully started scheduler: id={}, name={}",
                                    id.value,
                                    data.name
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to start scheduler id={}, name='{}': {}. Continuing with other schedulers.",
                                    id.value,
                                    data.name,
                                    e
                                );
                            }
                        }
                    }
                }
            }

            // スケジューラー全体を開始
            scheduler_manager.start().await?;
        }

        Ok(())
    }

    /// Slack handler を起動
    async fn start_slack_handler(&mut self) -> Result<()> {
        if let Some(slack_handler_manager) = &self.slack_handler_manager {
            // 環境変数からSLACK_APP_TOKENを取得
            match std::env::var("SLACK_APP_TOKEN") {
                Ok(app_token) if !app_token.is_empty() => {
                    let (_enabled_handlers, enabled_count) = {
                        let store = self.local_config_store.read().map_err(|e| {
                            anyhow::anyhow!("Failed to read lock config store: {}", e)
                        })?;

                        let handlers = store
                            .get_all_slack_event_handlers()
                            .into_iter()
                            .cloned()
                            .collect::<Vec<_>>();
                        let enabled_count = handlers
                            .iter()
                            .filter(|h| h.data.as_ref().is_some_and(|d| d.enabled))
                            .count();
                        (handlers, enabled_count)
                    };

                    tracing::info!(
                        "Starting Slack handler with {} enabled handlers from initial config",
                        enabled_count
                    );

                    // DynamicSlackHandlerManager を起動（Mutexでロック）
                    let mut manager = slack_handler_manager.lock().await;
                    if let Err(e) = manager.start().await {
                        tracing::error!("Failed to start Slack handler: {}. Continuing without Slack integration.", e);
                    } else {
                        tracing::info!(
                            "✅ Slack handler started successfully with {} enabled handlers",
                            enabled_count
                        );
                    }
                }
                _ => {
                    tracing::warn!(
                        "⚠️ SLACK_APP_TOKEN not set or empty. Slack handler will not start. Set SLACK_APP_TOKEN environment variable to enable Slack integration."
                    );
                }
            }
        } else {
            tracing::warn!("⚠️ DynamicSlackHandlerManager is not initialized");
        }

        Ok(())
    }

    async fn start_notification_listener(&self) -> Result<()> {
        tracing::info!("🚀 実際の通知リスナー処理を開始します");

        // 通知サービスからイベントレシーバーを取得
        let mut receiver =
            self.notification_service.subscribe().await.map_err(|e| {
                anyhow::anyhow!("Failed to subscribe to notification service: {}", e)
            })?;

        // ローカル設定ストアとscheduler_manager、listener_manager、slack_handler_managerのクローンを取得
        let local_config_store = Arc::clone(&self.local_config_store);
        let scheduler_manager = self.scheduler_manager.clone();
        let listener_manager = self.listener_manager.clone();
        let slack_handler_manager = self.slack_handler_manager.clone();

        // バックグラウンドタスクで通知処理を実行
        tokio::spawn(async move {
            tracing::info!("📡 設定変更通知受信ループを開始");

            while let Ok(Some(config_event)) = receiver.receive().await {
                if let Err(e) = Self::handle_config_change_event(
                    &scheduler_manager,
                    &listener_manager,
                    &slack_handler_manager,
                    &local_config_store,
                    config_event,
                )
                .await
                {
                    tracing::error!("❌ 設定変更処理でエラーが発生: {}", e);
                }
            }

            tracing::warn!("📡 設定変更通知受信ループが終了しました");
        });

        tracing::info!("✅ 通知リスナー開始完了");
        Ok(())
    }

    /// 設定変更イベントの処理
    /// ローカル設定ストアを更新し、必要に応じて動的管理コンポーネントに変更を通知
    async fn handle_config_change_event(
        scheduler_manager: &Option<Arc<crate::dynamic::scheduler_manager::DynamicSchedulerManager>>,
        listener_manager: &crate::dynamic::listener_manager::DynamicListenerManager,
        slack_handler_manager: &Option<
            Arc<tokio::sync::Mutex<slack_event_handler::DynamicSlackHandlerManager>>,
        >,
        local_config_store: &Arc<std::sync::RwLock<shared::LocalConfigStore>>,
        config_event: shared::config_events_proto::ConfigChangeEventWrapper,
    ) -> Result<()> {
        tracing::info!(
            "🔄 設定変更イベントを処理中: type={}, name={}, timestamp={}",
            config_event.action() as i32,
            config_event.entity_name(),
            config_event.timestamp_secs()
        );

        // ローカル設定ストアのスナップショット作成
        {
            let mut store = local_config_store.write().map_err(|e| {
                anyhow::anyhow!("Failed to acquire write lock on config store: {}", e)
            })?;

            let snapshot_message = format!(
                "Config change: {} {} at {}",
                config_event.action() as i32,
                config_event.entity_name(),
                config_event.timestamp_secs()
            );
            store.create_snapshot(snapshot_message);
        }

        // エンティティタイプに基づいて処理を分岐
        if config_event.is_cron_scheduler() {
            tracing::info!(
                "📅 CronScheduler変更イベント: {} (action: {:?})",
                config_event.entity_name(),
                config_event.action()
            );
            Self::handle_cron_scheduler_change(
                scheduler_manager,
                local_config_store,
                &config_event,
            )
            .await?;
        } else if config_event.is_worker_result_handler() {
            tracing::info!(
                "👂 WorkerResultHandler変更イベント: {} (action: {:?})",
                config_event.entity_name(),
                config_event.action()
            );
            Self::handle_worker_result_handler_change(
                listener_manager,
                local_config_store,
                &config_event,
            )
            .await?;
        } else if config_event.is_jobworkerp_server() {
            tracing::info!(
                "🖥️ JobworkerpServer変更イベント: {} (action: {:?})",
                config_event.entity_name(),
                config_event.action()
            );
            Self::handle_jobworkerp_server_change(local_config_store, &config_event).await?;
        } else if config_event.is_slack_event_handler() {
            tracing::info!(
                "💬 SlackEventHandler変更イベント: {} (action: {:?})",
                config_event.entity_name(),
                config_event.action()
            );
            Self::handle_slack_event_handler_change(
                slack_handler_manager,
                local_config_store,
                &config_event,
            )
            .await?;
        } else {
            tracing::warn!("⚠️ 未知の設定変更イベントタイプ");
            return Err(anyhow::anyhow!("Unknown config change event type"));
        }

        tracing::info!(
            "✅ 設定変更イベント処理完了: {}",
            config_event.entity_name()
        );
        Ok(())
    }

    /// CronScheduler変更の処理
    async fn handle_cron_scheduler_change(
        scheduler_manager: &Option<Arc<crate::dynamic::scheduler_manager::DynamicSchedulerManager>>,
        local_config_store: &Arc<std::sync::RwLock<shared::LocalConfigStore>>,
        config_event: &shared::config_events_proto::ConfigChangeEventWrapper,
    ) -> Result<()> {
        tracing::debug!("処理中: CronScheduler変更 - {}", config_event.entity_name());

        // ローカル設定ストアを更新
        {
            let mut store = local_config_store
                .write()
                .map_err(|e| anyhow::anyhow!("Failed to write lock config store: {}", e))?;

            // 設定イベントからローカルストアを更新（実データ使用）
            match config_event.action() {
                proto::jobworkerp_conductor::data::ChangeAction::Created
                | proto::jobworkerp_conductor::data::ChangeAction::Updated => {
                    if let Some(cron_event) = config_event.as_cron_scheduler() {
                        if let Some(scheduler_data) = &cron_event.data {
                            // 実際のイベントデータを使用
                            let scheduler = proto::jobworkerp_conductor::data::CronScheduler {
                                id: cron_event.id,
                                data: Some(scheduler_data.clone()),
                            };
                            store.upsert_cron_scheduler(scheduler)?;
                            tracing::info!(
                                "Updated CronScheduler from real data: {}",
                                scheduler_data.name
                            );
                        } else {
                            tracing::warn!(
                                "CronScheduler event has no data: {}",
                                config_event.entity_name()
                            );
                        }
                    }
                }
                proto::jobworkerp_conductor::data::ChangeAction::Deleted => {
                    if let Some(shared::config_events_proto::EntityId::CronScheduler(id)) =
                        config_event.typed_id()
                    {
                        store.remove_cron_scheduler(&id);
                        tracing::info!(
                            "Removed CronScheduler from local store: id={}, name={}",
                            id.value,
                            config_event.entity_name()
                        );
                    }
                }
                _ => {}
            }
        }

        // 🚀 DynamicSchedulerManager への動的更新を実装
        if let Some(scheduler_manager) = scheduler_manager {
            if let Err(e) = scheduler_manager
                .update_scheduler_from_event(config_event)
                .await
            {
                tracing::error!(
                    "❌ Failed to update scheduler dynamically: {}: {}",
                    config_event.entity_name(),
                    e
                );
            } else {
                tracing::info!(
                    "🔄 Successfully updated scheduler dynamically: {}",
                    config_event.entity_name()
                );
            }
        } else {
            tracing::warn!("⚠️ DynamicSchedulerManager is not initialized yet");
        }

        tracing::info!(
            "✅ CronScheduler変更処理完了: {}",
            config_event.entity_name()
        );
        Ok(())
    }

    /// WorkerResultHandler変更の処理
    async fn handle_worker_result_handler_change(
        listener_manager: &crate::dynamic::listener_manager::DynamicListenerManager,
        local_config_store: &Arc<std::sync::RwLock<shared::LocalConfigStore>>,
        config_event: &shared::config_events_proto::ConfigChangeEventWrapper,
    ) -> Result<()> {
        tracing::debug!(
            "処理中: WorkerResultHandler変更 - {}",
            config_event.entity_name()
        );

        // WorkerResultHandlerの設定変更処理を実装
        {
            let mut store = local_config_store
                .write()
                .map_err(|e| anyhow::anyhow!("Failed to write lock config store: {}", e))?;

            match config_event.action() {
                proto::jobworkerp_conductor::data::ChangeAction::Created
                | proto::jobworkerp_conductor::data::ChangeAction::Updated => {
                    if let Some(handler_event) = config_event.as_worker_result_handler() {
                        if let Some(handler_data) = &handler_event.data {
                            // 実際のイベントデータを使用
                            let handler = proto::jobworkerp_conductor::data::WorkerResultHandler {
                                id: handler_event.id,
                                data: Some(handler_data.clone()),
                            };
                            store.upsert_worker_result_handler(handler)?;
                            tracing::info!(
                                "Updated WorkerResultHandler from real data: {}",
                                handler_data.name
                            );
                        } else {
                            tracing::warn!(
                                "WorkerResultHandler event has no data: {}",
                                config_event.entity_name()
                            );
                        }
                    }
                }
                proto::jobworkerp_conductor::data::ChangeAction::Deleted => {
                    if let Some(shared::config_events_proto::EntityId::WorkerResultHandler(id)) =
                        config_event.typed_id()
                    {
                        store.remove_worker_result_handler(&id);
                        tracing::info!(
                            "Removed WorkerResultHandler from local store: id={}, name={}",
                            id.value,
                            config_event.entity_name()
                        );
                    }
                }
                _ => {}
            }
        }

        // DynamicListenerManager への動的更新を実装
        if let Err(e) = listener_manager
            .update_listener_from_event(config_event)
            .await
        {
            tracing::error!(
                "❌ Failed to update listener dynamically: {}: {}",
                config_event.entity_name(),
                e
            );
        } else {
            tracing::info!(
                "🔄 Successfully updated listener dynamically: {}",
                config_event.entity_name()
            );
        }

        tracing::info!(
            "✅ WorkerResultHandler変更処理完了: {}",
            config_event.entity_name()
        );

        Ok(())
    }

    /// JobworkerpServer変更の処理
    async fn handle_jobworkerp_server_change(
        local_config_store: &Arc<std::sync::RwLock<shared::LocalConfigStore>>,
        config_event: &shared::config_events_proto::ConfigChangeEventWrapper,
    ) -> Result<()> {
        tracing::debug!(
            "処理中: JobworkerpServer変更 - {}",
            config_event.entity_name()
        );

        // JobworkerpServerの設定変更処理を実装
        {
            let mut store = local_config_store
                .write()
                .map_err(|e| anyhow::anyhow!("Failed to write lock config store: {}", e))?;

            match config_event.action() {
                proto::jobworkerp_conductor::data::ChangeAction::Created
                | proto::jobworkerp_conductor::data::ChangeAction::Updated => {
                    if let Some(server_event) = config_event.as_jobworkerp_server() {
                        if let Some(server_data) = &server_event.data {
                            // 実際のイベントデータを使用
                            let server = proto::jobworkerp_conductor::data::JobworkerpServer {
                                id: server_event.id,
                                data: Some(server_data.clone()),
                            };
                            store.upsert_jobworkerp_server(server)?;
                            tracing::info!(
                                "Updated JobworkerpServer from real data: {}",
                                server_data.name
                            );
                        } else {
                            tracing::warn!(
                                "JobworkerpServer event has no data: {}",
                                config_event.entity_name()
                            );
                        }
                    }
                }
                proto::jobworkerp_conductor::data::ChangeAction::Deleted => {
                    if let Some(shared::config_events_proto::EntityId::JobworkerpServer(id)) =
                        config_event.typed_id()
                    {
                        store.remove_jobworkerp_server(&id);
                        tracing::info!(
                            "Removed JobworkerpServer from local store: id={}, name={}",
                            id.value,
                            config_event.entity_name()
                        );
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// SlackEventHandler変更の処理
    async fn handle_slack_event_handler_change(
        slack_handler_manager: &Option<
            Arc<tokio::sync::Mutex<slack_event_handler::DynamicSlackHandlerManager>>,
        >,
        local_config_store: &Arc<std::sync::RwLock<shared::LocalConfigStore>>,
        config_event: &shared::config_events_proto::ConfigChangeEventWrapper,
    ) -> Result<()> {
        tracing::debug!(
            "処理中: SlackEventHandler変更 - {}",
            config_event.entity_name()
        );

        // SlackEventHandlerの設定変更処理を実装
        {
            let mut store = local_config_store
                .write()
                .map_err(|e| anyhow::anyhow!("Failed to write lock config store: {}", e))?;

            match config_event.action() {
                proto::jobworkerp_conductor::data::ChangeAction::Created
                | proto::jobworkerp_conductor::data::ChangeAction::Updated => {
                    if let Some(handler_event) = config_event.as_slack_event_handler() {
                        if let Some(handler_data) = &handler_event.data {
                            // 実際のイベントデータを使用
                            let handler = proto::jobworkerp_conductor::data::SlackEventHandler {
                                id: handler_event.id,
                                data: Some(handler_data.clone()),
                            };
                            store.upsert_slack_event_handler(handler)?;
                            tracing::info!(
                                "Updated SlackEventHandler from real data: {}",
                                handler_data.name
                            );
                        } else {
                            tracing::warn!(
                                "SlackEventHandler event has no data: {}",
                                config_event.entity_name()
                            );
                        }
                    }
                }
                proto::jobworkerp_conductor::data::ChangeAction::Deleted => {
                    if let Some(shared::config_events_proto::EntityId::SlackEventHandler(id)) =
                        config_event.typed_id()
                    {
                        store.remove_slack_event_handler(&id);
                        tracing::info!(
                            "Removed SlackEventHandler from local store: id={}, name={}",
                            id.value,
                            config_event.entity_name()
                        );
                    }
                }
                _ => {}
            }
        }

        // DynamicSlackHandlerManager への動的更新を実装
        if let Some(slack_handler_manager) = slack_handler_manager {
            let manager = slack_handler_manager.lock().await;
            if let Err(e) = manager.update_handler_cache_from_event(config_event).await {
                tracing::error!(
                    "❌ Failed to update Slack handler cache dynamically: {}: {}",
                    config_event.entity_name(),
                    e
                );
            } else {
                tracing::info!(
                    "🔄 Successfully updated Slack handler cache dynamically: {}",
                    config_event.entity_name()
                );
            }
        } else {
            tracing::warn!("⚠️ DynamicSlackHandlerManager is not initialized yet");
        }

        tracing::info!(
            "✅ SlackEventHandler変更処理完了: {}",
            config_event.entity_name()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[test]
    fn test_initial_config_creation() {
        let config = InitialConfig::empty();
        assert_eq!(config.total_count(), 0);
        assert_eq!(config.cron_scheduler_count(), 0);
        assert_eq!(config.worker_result_handler_count(), 0);
        assert_eq!(config.jobworkerp_server_count(), 0);
    }

    #[test]
    fn test_initial_config_with_data() {
        let config = InitialConfig::new(vec![], vec![], vec![], vec![]);
        assert_eq!(config.total_count(), 0);
    }

    // MockConfigLoader for testing
    struct MockConfigLoader;

    #[async_trait]
    impl InitializationConfigLoader for MockConfigLoader {
        async fn load_all_cron_schedulers(&self) -> Result<Vec<CronScheduler>> {
            Ok(Vec::new())
        }

        async fn load_all_worker_result_handlers(&self) -> Result<Vec<WorkerResultHandler>> {
            Ok(Vec::new())
        }

        async fn load_all_jobworkerp_servers(&self) -> Result<Vec<JobworkerpServer>> {
            Ok(Vec::new())
        }

        async fn load_all_slack_event_handlers(&self) -> Result<Vec<SlackEventHandler>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn test_initialization_layer() {
        let config_loader = Arc::new(MockConfigLoader);
        let layer = InitializationLayer::new(config_loader);

        let initial_config = layer.load_initial_config().await.unwrap();
        assert_eq!(initial_config.total_count(), 0);
    }

    #[tokio::test]
    async fn test_initialization_layer_with_data() {
        // より実用的なモックデータでのテスト
        struct MockConfigLoaderWithData;

        #[async_trait]
        impl InitializationConfigLoader for MockConfigLoaderWithData {
            async fn load_all_cron_schedulers(&self) -> Result<Vec<CronScheduler>> {
                // テスト用の空のCronSchedulerを返す（実際のデータ構造テスト）
                Ok(Vec::new())
            }

            async fn load_all_worker_result_handlers(&self) -> Result<Vec<WorkerResultHandler>> {
                Ok(Vec::new())
            }

            async fn load_all_jobworkerp_servers(&self) -> Result<Vec<JobworkerpServer>> {
                Ok(Vec::new())
            }

            async fn load_all_slack_event_handlers(&self) -> Result<Vec<SlackEventHandler>> {
                Ok(Vec::new())
            }
        }

        let config_loader = Arc::new(MockConfigLoaderWithData);
        let layer = InitializationLayer::new(config_loader);

        let initial_config = layer.load_initial_config().await.unwrap();
        assert_eq!(initial_config.total_count(), 0);
        assert_eq!(initial_config.cron_scheduler_count(), 0);
        assert_eq!(initial_config.worker_result_handler_count(), 0);
        assert_eq!(initial_config.jobworkerp_server_count(), 0);
    }

    #[tokio::test]
    async fn test_event_handler_server_manager_lifecycle() {
        use shared::notification::MemoryNotificationService;

        // 通知サービスのモック作成
        let notification_service = Arc::new(MemoryNotificationService::new_default());

        // 初期設定作成
        let initial_config = InitialConfig::empty();

        // EventHandlerServerManager作成
        let mut server_manager = EventHandlerServerManager::new_with_initial_config(
            initial_config,
            notification_service,
            shared::noop_execution_ref_recorder(),
        )
        .unwrap();

        // 初期状態確認
        assert!(!server_manager.is_running());

        // 設定統計確認
        let stats = server_manager.get_config_stats().unwrap();
        assert_eq!(stats.total_count(), 0);
        assert_eq!(stats.version, 1);

        // サーバー開始
        server_manager.start().await.unwrap();
        assert!(server_manager.is_running());

        // 開始後の統計確認（スナップショット作成により変更なし）
        let stats = server_manager.get_config_stats().unwrap();
        assert_eq!(stats.snapshot_count, 1); // Initial startup snapshot

        // サーバー停止
        server_manager.stop().await.unwrap();
        assert!(!server_manager.is_running());

        // 停止後の統計確認（停止時スナップショット追加）
        let stats = server_manager.get_config_stats().unwrap();
        assert_eq!(stats.snapshot_count, 2); // Startup + shutdown snapshots
    }

    #[tokio::test]
    async fn test_event_handler_server_manager_double_start() {
        use shared::notification::MemoryNotificationService;

        let notification_service = Arc::new(MemoryNotificationService::new_default());
        let initial_config = InitialConfig::empty();

        let mut server_manager = EventHandlerServerManager::new_with_initial_config(
            initial_config,
            notification_service,
            shared::noop_execution_ref_recorder(),
        )
        .unwrap();

        // 最初の開始は成功
        server_manager.start().await.unwrap();
        assert!(server_manager.is_running());

        // 2回目の開始はエラー
        let result = server_manager.start().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already running"));
    }
}
