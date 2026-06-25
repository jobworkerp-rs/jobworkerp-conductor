use crate::app::notification::ConfigChangeNotificationServiceImpl;

use super::app::config_management::ConfigManagementAppImpl;
use super::app::cron_scheduler::CronSchedulerAppImpl;
use super::app::execution_status::ExecutionStatusAppImpl;
use super::app::jobworkerp_server::JobworkerpServerAppImpl;
use super::app::slack_event_handler::SlackEventHandlerAppImpl;
use super::app::source_resolver::CronSourceResolver;
use super::app::worker_result_handler::WorkerResultHandlerAppImpl;
use infra::infra::module::RepositoryModule;
use memory_utils::cache::stretto::{new_memory_cache, MemoryCacheConfig};
use proto::jobworkerp_conductor::data::{
    CronScheduler, JobworkerpServer, SlackEventHandler, WorkerResultHandler,
};
use shared::notification::service::ConfigChangeNotificationService;
use std::sync::Arc;

// Re-export InitializationConfigLoaderImpl from separate file
pub use crate::initialization_config_loader::InitializationConfigLoaderImpl;

pub struct AppModule {
    pub worker_result_handler_app: Arc<WorkerResultHandlerAppImpl>,
    pub jobworkerp_server_app: Arc<JobworkerpServerAppImpl>,
    pub cron_scheduler_app: Arc<CronSchedulerAppImpl>,
    pub slack_event_handler_app: Arc<SlackEventHandlerAppImpl>,
    pub config_management_app: Arc<ConfigManagementAppImpl>,
    pub execution_status_app: Arc<ExecutionStatusAppImpl>,
    /// Phase 4: NotificationServiceをDI原則に従ってAppModule内で管理
    pub notification_service: Arc<dyn ConfigChangeNotificationService>,
}

impl AppModule {
    pub async fn new_by_env(repositories: RepositoryModule) -> Self {
        // TODO memory cache をinfraでも利用する場合はinfra層でモジュール化しておく
        let mc_config = envy::prefixed("MEMORY_CACHE_")
            .from_env::<MemoryCacheConfig>()
            .expect("Error on loading memory cache config");

        // Phase 4: NotificationServiceをDI原則に従ってAppModule内で作成
        let notification_service = Arc::new(
            ConfigChangeNotificationServiceImpl::new_by_env()
                .expect("Failed to create notification service"),
        );

        // App実装を作成（コンストラクタ注入でNotificationServiceを渡す）
        let worker_result_handler_app = WorkerResultHandlerAppImpl::new(
            repositories.worker_result_handler_repository,
            new_memory_cache::<Arc<String>, WorkerResultHandler>(&mc_config),
            notification_service.clone(),
        );

        let jobworkerp_server_app = JobworkerpServerAppImpl::new(
            repositories.jobworkerp_server_repository.clone(),
            new_memory_cache::<Arc<String>, JobworkerpServer>(&mc_config),
            notification_service.clone(),
        );

        // Arc the cron app up front: the execution-status source resolver shares it (one-way:
        // ExecutionStatus → Cron), so it must exist before the execution-status app is built.
        let cron_scheduler_app = Arc::new(CronSchedulerAppImpl::new(
            repositories.cron_scheduler_repository,
            new_memory_cache::<Arc<String>, CronScheduler>(&mc_config),
            notification_service.clone(),
        ));
        let slack_event_handler_app = SlackEventHandlerAppImpl::new(
            repositories.slack_event_handler_repository,
            repositories.jobworkerp_server_repository.clone(),
            new_memory_cache::<Arc<String>, SlackEventHandler>(&mc_config),
            notification_service.clone(),
        );

        // Manual-trigger source resolver: holds a one-way Arc to the cron app (shared below).
        let source_resolver = Arc::new(CronSourceResolver::new(
            cron_scheduler_app.clone(),
            repositories.jobworkerp_server_repository.clone(),
        ));

        AppModule {
            worker_result_handler_app: Arc::new(worker_result_handler_app),
            jobworkerp_server_app: Arc::new(jobworkerp_server_app),
            cron_scheduler_app,
            slack_event_handler_app: Arc::new(slack_event_handler_app),
            config_management_app: Arc::new(ConfigManagementAppImpl::new(
                repositories.config_management_repository,
            )),
            execution_status_app: Arc::new(ExecutionStatusAppImpl::new(
                repositories.execution_ref_repository,
                repositories.jobworkerp_server_repository.clone(),
                source_resolver,
            )),
            notification_service,
        }
    }
}
