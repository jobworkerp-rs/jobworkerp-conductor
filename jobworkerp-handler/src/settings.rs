use anyhow::Result;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use std::fmt;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct JobResultListenerSetting {
    pub handler_id: Option<i64>,
    pub name: String,
    pub listen_jobworkerp: Arc<JobworkerpClientWrapper>,
    pub listen_worker_name: String,
    pub process_jobworkerp: Arc<JobworkerpClientWrapper>,
    pub workflow_url: String,
    pub channel: Option<String>,
    pub args: Option<String>,
    pub worker_name: Option<String>,
    pub using: Option<String>,
    pub process_jobworkerp_server_id: Option<i64>,
    pub execution_ref_recorder: shared::SharedExecutionRefRecorder,
}

impl fmt::Debug for JobResultListenerSetting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobResultListenerSetting")
            .field("handler_id", &self.handler_id)
            .field("name", &self.name)
            .field("listen_jobworkerp", &self.listen_jobworkerp)
            .field("listen_worker_name", &self.listen_worker_name)
            .field("process_jobworkerp", &self.process_jobworkerp)
            .field("workflow_url", &self.workflow_url)
            .field("channel", &self.channel)
            .field("args", &self.args)
            .field("worker_name", &self.worker_name)
            .field("using", &self.using)
            .field(
                "process_jobworkerp_server_id",
                &self.process_jobworkerp_server_id,
            )
            .finish()
    }
}

impl JobResultListenerSetting {
    pub fn new_from(
        workflow_setting_item: WorkflowSettingItem,
        jobworkerp_map: &HashMap<String, Arc<JobworkerpClientWrapper>>,
    ) -> Result<Self> {
        let jobworkerp_address = jobworkerp_map
            .get(&workflow_setting_item.jobworkerp)
            .ok_or(anyhow::anyhow!(
                "jobworkerp not found:{} from {:#?}",
                &workflow_setting_item.jobworkerp,
                &workflow_setting_item,
            ))?
            .clone();
        let listen_jobworkerp_name = workflow_setting_item
            .listen_jobworkerp
            .ok_or(anyhow::anyhow!("listen_jobworkerp setting is None"))?;
        let listen_jobworkerp = jobworkerp_map
            .get(&listen_jobworkerp_name)
            .ok_or(anyhow::anyhow!(
                "listen_jobworkerp not found:{}",
                &listen_jobworkerp_name,
            ))?
            .clone();
        let listen_worker_name = workflow_setting_item
            .listen_worker_name
            .clone()
            .ok_or(anyhow::anyhow!("listen_worker_name is None"))?;
        // Validate: either worker_name or workflow_url must be specified
        let has_worker_name = workflow_setting_item
            .worker_name
            .as_ref()
            .is_some_and(|n| !n.is_empty());
        if !has_worker_name && workflow_setting_item.workflow_url.is_empty() {
            return Err(anyhow::anyhow!(
                "Either worker_name or workflow_url must be specified for listener: {:#?}",
                workflow_setting_item.name
            ));
        }
        Ok(Self {
            handler_id: None,
            name: workflow_setting_item.name,
            listen_jobworkerp,
            listen_worker_name,
            process_jobworkerp: jobworkerp_address,
            workflow_url: workflow_setting_item.workflow_url,
            channel: workflow_setting_item.channel,
            args: workflow_setting_item.args,
            worker_name: workflow_setting_item.worker_name,
            using: workflow_setting_item.using,
            process_jobworkerp_server_id: None,
            execution_ref_recorder: shared::noop_execution_ref_recorder(),
        })
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct WorkflowSettingItem {
    pub name: String,
    pub jobworkerp: String,
    #[serde(default)]
    pub workflow_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    // for job result listener
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_worker_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_jobworkerp: Option<String>,
    // for scheduler
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crontab: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub using: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct JobWorkerpSetting {
    pub name: String,
    pub address: String,
}

#[derive(Debug, Clone)]
pub struct SchedulerSetting {
    pub name: String,
    pub jobworkerp: Arc<JobworkerpClientWrapper>,
    pub workflow_url: String,
    pub channel: Option<String>,
    pub crontab: String,
    pub args: Option<String>,
    pub worker_name: Option<String>,
    pub using: Option<String>,
}

impl SchedulerSetting {
    fn new_from(
        item: WorkflowSettingItem,
        jobworkerp_map: &HashMap<String, Arc<JobworkerpClientWrapper>>,
    ) -> Result<Self> {
        let crontab = item
            .crontab
            .clone()
            .ok_or(anyhow::anyhow!("crontab is None: {:#?}", item))?;
        let has_worker_name = item.worker_name.as_ref().is_some_and(|n| !n.is_empty());
        if !has_worker_name && item.workflow_url.is_empty() {
            return Err(anyhow::anyhow!(
                "Either worker_name or workflow_url must be specified: {:#?}",
                item.name
            ));
        }
        let jobworkerp = jobworkerp_map
            .get(&item.jobworkerp)
            .ok_or(anyhow::anyhow!(
                "jobworkerp not found:{} from {:#?}",
                &item.jobworkerp,
                item.jobworkerp,
            ))?
            .clone();
        Ok(SchedulerSetting {
            name: item.name,
            jobworkerp,
            workflow_url: item.workflow_url,
            channel: item.channel,
            crontab,
            args: item.args,
            worker_name: item.worker_name,
            using: item.using,
        })
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct WorkflowSettingFile {
    pub jobworkerp: Vec<JobWorkerpSetting>,
    pub listeners: Option<Vec<WorkflowSettingItem>>,
    pub schedulers: Option<Vec<WorkflowSettingItem>>,
}

#[derive(Debug, Clone)]
pub struct WorkflowSettings {
    pub jobworkerp_map: HashMap<String, Arc<JobworkerpClientWrapper>>,
    pub listeners: Vec<JobResultListenerSetting>,
    pub schedulers: Vec<SchedulerSetting>,
}
impl WorkflowSettings {
    // const JOBWORKERP_KEY: &'static str = "jobworkerp";
    // const LISTENER_KEY: &'static str = "listeners";
    // const SCHEDULER_KEY: &'static str = "schedulers";

    pub fn new(
        listeners: Vec<JobResultListenerSetting>,
        schedulers: Vec<SchedulerSetting>,
        jobworkerp_map: HashMap<String, Arc<JobworkerpClientWrapper>>,
    ) -> Self {
        Self {
            jobworkerp_map,
            listeners,
            schedulers,
        }
    }
    pub async fn load_from_toml(toml_str: &str) -> Result<Self> {
        let settings: WorkflowSettingFile = toml::from_str(toml_str)
            .inspect_err(|e| tracing::error!("toml parse error: {:?}", e))?;
        // let mut settings: HashMap<String, Vec<WorkflowSettingItem>> = toml::from_str(toml_str)
        //     .inspect_err(|e| tracing::error!("toml parse error: {:?}", e))?;
        let mut jobworkerp_map = HashMap::new();
        for j in settings.jobworkerp.into_iter() {
            let jobworkerp = JobworkerpClientWrapper::new(&j.address, None).await?;
            jobworkerp_map.insert(j.name.clone(), Arc::new(jobworkerp));
        }
        let listeners = settings
            .listeners
            .unwrap_or_default()
            .into_iter()
            .map(|l| JobResultListenerSetting::new_from(l, &jobworkerp_map))
            .collect::<Result<Vec<_>>>()?;
        let schedulers = settings
            .schedulers
            .unwrap_or_default()
            .into_iter()
            .map(|s| SchedulerSetting::new_from(s, &jobworkerp_map))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::new(listeners, schedulers, jobworkerp_map))
    }
    pub async fn load_from_toml_file(file_path: &str) -> Result<Self> {
        let toml_str = std::fs::read_to_string(file_path)
            .inspect_err(|e| tracing::error!("read file error: {:?}", e))?;
        Self::load_from_toml(&toml_str).await
    }
}
