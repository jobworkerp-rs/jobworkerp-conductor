use proto::jobworkerp_conductor::data::{
    cron_scheduler_data::ExecutionTarget, CronScheduler, CronSchedulerData, CronSchedulerId,
    WorkerExecution, WorkflowExecution,
};

pub(crate) fn job_timeout_sec_from_db(
    value: Option<i64>,
) -> Result<Option<u32>, std::num::TryFromIntError> {
    value.map(u32::try_from).transpose()
}

// db row definitions
#[derive(sqlx::FromRow)]
pub struct CronSchedulerRow {
    pub id: i64,
    pub name: String,
    pub jobworkerp_server_id: i64,
    pub workflow_url: String,
    pub channel: Option<String>,
    pub crontab: String,
    pub enabled: bool,
    pub description: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub args: Option<String>,
    pub worker_name: Option<String>,
    pub using: Option<String>,
    pub job_timeout_sec: Option<i64>,
}

impl CronSchedulerRow {
    pub fn to_proto(&self) -> anyhow::Result<CronScheduler> {
        let job_timeout_sec = job_timeout_sec_from_db(self.job_timeout_sec).map_err(|_| {
            anyhow::anyhow!("invalid cron job_timeout_sec: {:?}", self.job_timeout_sec)
        })?;
        // Build oneof execution_target from DB columns
        let execution_target = if self.worker_name.as_ref().is_some_and(|n| !n.is_empty()) {
            Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: self.worker_name.clone().unwrap_or_default(),
                r#using: self.r#using.clone(),
            }))
        } else if !self.workflow_url.is_empty() {
            Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: self.workflow_url.clone(),
                channel: self.channel.clone(),
            }))
        } else {
            None
        };

        Ok(CronScheduler {
            id: Some(CronSchedulerId { value: self.id }),
            data: Some(CronSchedulerData {
                name: self.name.clone(),
                jobworkerp_server_id: Some(proto::jobworkerp_conductor::data::JobworkerpServerId {
                    value: self.jobworkerp_server_id,
                }),
                // Backward compat: populate deprecated fields 3/4 for old clients.
                // On write, only execution_target is authoritative.
                workflow_url: self.workflow_url.clone(),
                channel: self.channel.clone(),
                crontab: self.crontab.clone(),
                enabled: self.enabled,
                description: self.description.clone(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                args: self.args.clone(),
                job_timeout_sec,
                execution_target,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CronSchedulerRow;

    fn row(job_timeout_sec: Option<i64>) -> CronSchedulerRow {
        CronSchedulerRow {
            id: 1,
            name: "test".to_string(),
            jobworkerp_server_id: 1,
            workflow_url: String::new(),
            channel: None,
            crontab: String::new(),
            enabled: true,
            description: None,
            created_at: 0,
            updated_at: 0,
            args: None,
            worker_name: None,
            using: None,
            job_timeout_sec,
        }
    }

    #[test]
    fn rejects_invalid_persisted_timeout_values() {
        assert!(row(Some(-1)).to_proto().is_err());
        assert!(row(Some(i64::from(u32::MAX) + 1)).to_proto().is_err());
    }
}
