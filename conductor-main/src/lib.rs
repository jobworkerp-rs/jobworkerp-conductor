use anyhow::{Context, Result};
use cron_scheduler::CronScheduler;
use jobworkerp_handler::settings::WorkflowSettings;
use tokio::task::JoinSet;

pub mod bootstrap;
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
        jset.spawn(self.jobworkerp_result_listener.listen_all());
        jset.spawn(self.cron_scheduler.start());

        // Listeners never finish on their own, so the process decides when to
        // stop; dropping the set aborts whatever is still running.
        tokio::select! {
            res = jset.join_all() => tracing::info!("res: {:?}", res),
            _ = command_utils::util::shutdown::shutdown_signal() => {
                tracing::info!("Shutdown signal received");
            }
        }
        Ok(())
    }
}
