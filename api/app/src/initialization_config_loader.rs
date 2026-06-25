use super::app::cron_scheduler::CronSchedulerApp;
use super::app::jobworkerp_server::JobworkerpServerApp;
use super::app::slack_event_handler::SlackEventHandlerApp;
use super::app::worker_result_handler::WorkerResultHandlerApp;
use super::module::AppModule;
use anyhow::Result;
use proto::jobworkerp_conductor::data::{
    CronScheduler, JobworkerpServer, SlackEventHandler, WorkerResultHandler,
};
use std::sync::Arc;

/// 初期設定専用のローダー実装
/// AppModuleをコンポジションで利用し、shared層のInitializationConfigLoaderトレイトを実装
pub struct InitializationConfigLoaderImpl {
    app_module: Arc<AppModule>,
}

impl InitializationConfigLoaderImpl {
    pub fn new(app_module: Arc<AppModule>) -> Self {
        Self { app_module }
    }
}

/// InitializationConfigLoaderImplにshared層のInitializationConfigLoaderトレイトを実装
/// クリーンアーキテクチャに従い、shared層のインターフェースを実装
#[async_trait::async_trait]
impl shared::initialization::InitializationConfigLoader for InitializationConfigLoaderImpl {
    async fn load_all_cron_schedulers(&self) -> Result<Vec<CronScheduler>> {
        self.app_module
            .cron_scheduler_app
            .find_cron_scheduler_all_list(None)
            .await
    }

    async fn load_all_worker_result_handlers(&self) -> Result<Vec<WorkerResultHandler>> {
        self.app_module
            .worker_result_handler_app
            .find_worker_result_handler_all_list(None)
            .await
    }

    async fn load_all_jobworkerp_servers(&self) -> Result<Vec<JobworkerpServer>> {
        self.app_module
            .jobworkerp_server_app
            .find_jobworkerp_server_all_list(None)
            .await
    }

    async fn load_all_slack_event_handlers(&self) -> Result<Vec<SlackEventHandler>> {
        self.app_module
            .slack_event_handler_app
            .find_slack_event_handler_list()
            .await
    }
}
