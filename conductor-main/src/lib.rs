use anyhow::{Context, Result};
use cron_scheduler::CronScheduler;
use jobworkerp_handler::settings::WorkflowSettings;
use tokio::task::JoinSet;

pub mod cron_scheduler;

pub struct ConductorServer {
    jobworkerp_result_listener: jobworkerp_handler::job_result_listener::JobworkerpResultListener,
    cron_scheduler: CronScheduler,
}

impl ConductorServer {
    pub async fn new(workflow_map: WorkflowSettings) -> Result<Self> {
        // TODO workflow_map.scheduler is not used
        let jobworkerp_result_listener =
            jobworkerp_handler::job_result_listener::JobworkerpResultListener::new(
                workflow_map.listeners,
            )
            .await
            .context("Failed to create jobworkerp_result_listener")?;
        let cron_scheduler = CronScheduler::new(workflow_map.schedulers).await?;
        Ok(Self {
            jobworkerp_result_listener,
            cron_scheduler,
        })
    }
    pub async fn serve(&'static self) -> Result<()> {
        let mut jset = JoinSet::new();
        // let jh = tokio::spawn(self.jobworkerp_result_listener
        //     .listen_all());
        // let cs = tokio::spawn(self.cron_scheduler
        //     .start());
        // let res = join_all!(vec![jh, cs]).await;
        jset.spawn(self.jobworkerp_result_listener.listen_all());
        jset.spawn(self.cron_scheduler.start());

        let res = jset.join_all().await;
        tracing::info!("res: {:?}", res);
        Ok(())
    }
}
