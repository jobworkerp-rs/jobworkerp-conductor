use anyhow::Result;
use proto::jobworkerp_conductor::data::{
    CronScheduler, JobworkerpServer, SlackEventHandler, WorkerResultHandler,
};

/// 初期設定ローダーのインターフェース
///
/// 各種設定データをデータベースから読み込むための抽象化レイヤー。
/// クリーンアーキテクチャの原則に従い、shared層でインターフェースを定義し、
/// api層で実装、jobworkerp-conductor層で利用する。
#[async_trait::async_trait]
pub trait InitializationConfigLoader: Send + Sync {
    /// すべてのCronSchedulerを取得
    async fn load_all_cron_schedulers(&self) -> Result<Vec<CronScheduler>>;

    /// すべてのWorkerResultHandlerを取得
    async fn load_all_worker_result_handlers(&self) -> Result<Vec<WorkerResultHandler>>;

    /// すべてのJobworkerpServerを取得
    async fn load_all_jobworkerp_servers(&self) -> Result<Vec<JobworkerpServer>>;

    /// すべてのSlackEventHandlerを取得
    async fn load_all_slack_event_handlers(&self) -> Result<Vec<SlackEventHandler>>;
}
