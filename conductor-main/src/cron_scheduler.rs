use anyhow::Result;
use jobworkerp_handler::settings::SchedulerSetting;
use shared::workflow_executor;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct CronScheduler {
    scheduler: tokio_cron_scheduler::JobScheduler,
    settings: Vec<SchedulerSetting>,
    guids: Arc<Mutex<Vec<Uuid>>>,
}

impl CronScheduler {
    pub async fn new(scheduler_settings: Vec<SchedulerSetting>) -> Result<Self> {
        // no timeout for listening job result stream (infinite)
        let (scheduler, settings, guids) =
            Self::setup_cron_scheduler(scheduler_settings.clone()).await?;
        Ok(Self {
            scheduler,
            settings,
            guids: Arc::new(Mutex::new(guids)),
        })
    }

    // only for new instance (guids is empty)
    async fn setup_cron_scheduler(
        schdule_settings: Vec<SchedulerSetting>,
    ) -> Result<(
        tokio_cron_scheduler::JobScheduler,
        Vec<SchedulerSetting>,
        Vec<Uuid>,
    )> {
        let mut scheduler = tokio_cron_scheduler::JobScheduler::new().await?;
        let mut settings = vec![];
        let mut guids = vec![];
        scheduler.shutdown_on_ctrl_c();

        scheduler.set_shutdown_handler(Box::new(|| {
            Box::pin(async move {
                tracing::info!("cron schduler shutdown done");
            })
        }));
        for setting in schdule_settings.into_iter() {
            // clone arc for closure
            let setting_arc = Arc::new(setting.clone());
            match tokio_cron_scheduler::Job::new_async(
                setting_arc.crontab.clone(),
                move |uuid, mut l| {
                    let sarc = setting_arc.clone();
                    Box::pin(async move {
                        tracing::info!("execute job: {:?}", sarc.name);
                        let result = if let Some(ref wname) = sarc.worker_name {
                            // Worker execution mode
                            workflow_executor::execute_worker_by_name(
                                wname,
                                sarc.jobworkerp.address(),
                                sarc.args.as_deref(),
                                sarc.using.as_deref(),
                            )
                            .await
                        } else {
                            // URL execution mode (fix: use args instead of hardcoded "{}")
                            let input = sarc.args.as_deref().unwrap_or("{}");
                            sarc.jobworkerp
                                .execute_workflow(
                                    None,
                                    Arc::new(std::collections::HashMap::new()),
                                    &sarc.workflow_url,
                                    input,
                                    sarc.channel.as_deref(),
                                )
                                .await
                                .map(|_| ())
                        };
                        match result {
                            Ok(()) => {
                                tracing::info!("execute job done: {:?}", sarc.name);
                            }
                            Err(e) => {
                                tracing::error!("execute job failed: {:?}: {}", sarc.name, e);
                            }
                        }
                        // Query the next execution time for this job
                        let next_tick = l.next_tick_for_job(uuid).await;
                        match next_tick {
                            Ok(Some(ts)) => tracing::info!("Next time for job is {:?}", ts),
                            _ => {
                                tracing::error!("Could not get next tick for job: {:?}", sarc.name)
                            }
                        }
                    })
                },
            ) {
                Ok(job) => match scheduler.add(job).await {
                    Ok(guid) => {
                        guids.push(guid);
                        settings.push(setting);
                    }
                    Err(e) => {
                        tracing::error!("failed to add job: {:?}", e);
                    }
                },
                Err(e) => {
                    tracing::error!("failed to add job: {:?}", e);
                }
            }
        }

        Ok((scheduler, settings, guids))
    }

    pub async fn start(&self) -> Result<()> {
        // tick every 500 msec for cron job
        if self.scheduler.inited().await {
            self.scheduler.start().await?;
            // wait for ctrl-c
            tokio::signal::ctrl_c().await?;
        } else {
            tracing::info!("cron scheduler not inited");
        }
        Ok(())
    }
    pub async fn stop(&mut self, name: &str) -> Result<bool> {
        let mut guids = self.guids.lock().await;
        let mut idx = None;
        for (i, setting) in self.settings.iter().enumerate() {
            if setting.name == name {
                idx = Some(i);
                break;
            }
        }
        if let Some(i) = idx {
            if let Some(guid) = guids.get(i) {
                self.scheduler.remove(guid).await?;
                guids.remove(i);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub async fn stop_all(&mut self) -> Result<()> {
        self.scheduler.shutdown().await?;
        self.guids.lock().await.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
    use jobworkerp_handler::settings::JobWorkerpSetting;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::Duration;
    use tracing::Level;

    #[ignore = "need local jobworkerp server"]
    #[tokio::test]
    async fn test_cron_scheduler() {
        command_utils::util::tracing::tracing_init_test(Level::DEBUG);
        let mut jobworkerp_map = HashMap::new();
        let jobworkerp = JobWorkerpSetting {
            name: "test".to_string(),
            address: "http://localhost:9000".to_string(),
        };
        let jobworkerp = Arc::new(
            JobworkerpClientWrapper::new(&jobworkerp.address, None)
                .await
                .unwrap(),
        );
        jobworkerp_map.insert("test".to_string(), jobworkerp.clone());

        let setting = SchedulerSetting {
            name: "test".to_string(),
            jobworkerp: jobworkerp.clone(),
            workflow_url: "../workflows/echo-test.yml".to_string(),
            channel: None,
            crontab: "*/1 * * * * *".to_string(),
            args: None,
            worker_name: None,
            using: None,
        };
        let scheduler = CronScheduler::new(vec![setting.clone()]).await.unwrap();
        let guids = scheduler.guids.lock().await;
        assert_eq!(guids.len(), 1);
        let _guid = guids[0];
        drop(guids);

        let mut scheduler = scheduler;
        let scheduler1 = scheduler.clone();

        tokio::spawn(async move {
            scheduler1.start().await.unwrap();
        });
        tokio::time::sleep(Duration::from_secs(2)).await;
        scheduler.stop(&setting.name).await.unwrap();
        let guids = scheduler.guids.lock().await;
        assert_eq!(guids.len(), 0);
        drop(guids);

        let setting = SchedulerSetting {
            name: "test".to_string(),
            jobworkerp,
            workflow_url: "../workflows/echo-test.yml".to_string(),
            channel: None,
            crontab: "*/1 * * * * *".to_string(),
            args: None,
            worker_name: None,
            using: None,
        };
        let mut scheduler = CronScheduler::new(vec![setting.clone()]).await.unwrap();
        let guids = scheduler.guids.lock().await;
        assert_eq!(guids.len(), 1);
        let _guid = guids[0];
        drop(guids);

        let scheduler1 = scheduler.clone();
        tokio::spawn(async move { scheduler1.start().await.unwrap() });
        tokio::time::sleep(Duration::from_secs(2)).await;
        scheduler.stop_all().await.unwrap();
        let guids = scheduler.guids.lock().await;
        assert_eq!(guids.len(), 0);
        drop(guids);
    }
}
