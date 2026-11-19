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
                workflow_setting_item.jobworkerp,
                workflow_setting_item,
            ))?
            .clone();
        let listen_jobworkerp_name = workflow_setting_item
            .listen_jobworkerp
            .ok_or(anyhow::anyhow!("listen_jobworkerp setting is None"))?;
        let listen_jobworkerp = jobworkerp_map
            .get(&listen_jobworkerp_name)
            .ok_or(anyhow::anyhow!(
                "listen_jobworkerp not found:{}",
                listen_jobworkerp_name,
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

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_timeout_sec: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Absent means enabled, as in files written before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

impl WorkflowSettingItem {
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct JobWorkerpSetting {
    pub name: String,
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Absent means enabled, as in files written before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

impl JobWorkerpSetting {
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }
}

/// A Slack event handler in the settings file. Only the DB-driven mode reads
/// it; the server is referenced by name like the other items.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SlackHandlerSettingItem {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Absent means enabled, as for the other items.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    pub jobworkerp: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack_channel_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_pattern: Option<String>,
    #[serde(default)]
    pub mention_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reaction_names: Option<String>,
    /// "added", "removed" or "both".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reaction_operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reaction_user_filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub using: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_sec: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,
}

impl SlackHandlerSettingItem {
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }
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
                item.jobworkerp,
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
    /// May be empty: an import can name servers that are already stored.
    #[serde(default)]
    pub jobworkerp: Vec<JobWorkerpSetting>,
    pub listeners: Option<Vec<WorkflowSettingItem>>,
    pub schedulers: Option<Vec<WorkflowSettingItem>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack_handlers: Option<Vec<SlackHandlerSettingItem>>,
}

impl WorkflowSettingFile {
    pub fn parse(toml_str: &str) -> Result<Self> {
        toml::from_str(toml_str).map_err(|e| anyhow::anyhow!("Invalid TOML format: {e}"))
    }

    /// The file as the legacy (TOML-driven) mode runs it. An exported file
    /// carries disabled items, which must not start; servers stay, since the
    /// remaining items may still name them.
    pub fn parse_for_legacy(toml_str: &str) -> Result<Self> {
        let mut file =
            Self::parse(toml_str).inspect_err(|e| tracing::error!("toml parse error: {:?}", e))?;
        let enabled_only = |items: Option<Vec<WorkflowSettingItem>>| {
            items.map(|items| items.into_iter().filter(|i| i.is_enabled()).collect())
        };
        file.listeners = enabled_only(file.listeners);
        file.schedulers = enabled_only(file.schedulers);
        if file.slack_handlers.as_ref().is_some_and(|s| !s.is_empty()) {
            tracing::warn!("slack_handlers are ignored in legacy mode");
        }
        Ok(file)
    }
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
        let settings = WorkflowSettingFile::parse_for_legacy(toml_str)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    const SERVER: &str = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:9000"
"#;

    #[test]
    fn reads_files_written_before_description_enabled_and_slack() {
        let toml = format!(
            "{SERVER}
[[schedulers]]
name = \"job\"
jobworkerp = \"server1\"
workflow_url = \"job.yml\"
crontab = \"0 0 * * * *\"
"
        );
        let file: WorkflowSettingFile = toml::from_str(&toml).unwrap();
        let scheduler = &file.schedulers.unwrap()[0];
        assert_eq!(scheduler.description, None);
        assert!(scheduler.is_enabled());
        assert!(file.jobworkerp[0].is_enabled());
        assert!(file.slack_handlers.is_none());
    }

    #[test]
    fn round_trips_description_enabled_and_slack_handlers() {
        let toml = r#"[[jobworkerp]]
name = "server1"
address = "http://localhost:9000"
description = "main"
enabled = false

[[slack_handlers]]
name = "deploy"
description = "on deploy messages"
enabled = false
jobworkerp = "server1"
slack_channel_id = "C123"
message_pattern = "^deploy"
mention_required = true
worker_name = "deployer"
using = "run"
timeout_sec = 600
args = '{"a":1}'

[[slack_handlers]]
name = "thumbs"
jobworkerp = "server1"
reaction_names = "+1"
reaction_operation = "added"
workflow_url = "https://example.com/wf.yml"
channel = "batch"
"#;
        let file: WorkflowSettingFile = toml::from_str(toml).unwrap();
        assert_eq!(file.jobworkerp[0].description.as_deref(), Some("main"));
        assert!(!file.jobworkerp[0].is_enabled());
        let slack = file.slack_handlers.clone().unwrap();
        assert_eq!(slack.len(), 2);
        assert!(!slack[0].is_enabled());
        assert_eq!(slack[0].timeout_sec, Some(600));
        assert!(slack[0].mention_required);
        assert_eq!(slack[1].reaction_operation.as_deref(), Some("added"));
        assert!(!slack[1].mention_required);

        let again: WorkflowSettingFile =
            toml::from_str(&toml::to_string_pretty(&file).unwrap()).unwrap();
        assert_eq!(again.slack_handlers.unwrap().len(), 2);
        assert_eq!(again.jobworkerp[0].description.as_deref(), Some("main"));
    }

    const SCHEDULER: &str = r#"
[[jobworkerp]]
name = "server1"
address = "http://localhost:8080"

[[schedulers]]
name = "job"
jobworkerp = "server1"
workflow_url = "job.yml"
crontab = "0 * * * *"
"#;

    #[test]
    fn job_timeout_accepts_omitted_and_the_unsigned_range() {
        let parsed = WorkflowSettingFile::parse(SCHEDULER).unwrap();
        assert_eq!(parsed.schedulers.unwrap()[0].job_timeout_sec, None);
        for (value, expected) in [("0", 0), ("4294967295", u32::MAX)] {
            // [[schedulers]] is the last table, so the appended key belongs to it.
            let toml = format!("{SCHEDULER}job_timeout_sec = {value}\n");
            let item = WorkflowSettingFile::parse(&toml)
                .unwrap()
                .schedulers
                .unwrap()
                .remove(0);
            assert_eq!(item.job_timeout_sec, Some(expected));
        }
    }

    #[test]
    fn job_timeout_rejects_negative_and_out_of_range_values() {
        for value in ["-1", "4294967296"] {
            let toml = format!("{SCHEDULER}job_timeout_sec = {value}\n");
            assert!(WorkflowSettingFile::parse(&toml).is_err());
        }
        assert!(WorkflowSettingFile::parse("[[jobworkerp]\nname = ").is_err());
    }

    #[test]
    fn reads_a_file_naming_only_servers_already_stored() {
        let file = WorkflowSettingFile::parse(
            r#"
[[schedulers]]
name = "job"
jobworkerp = "stored"
worker_name = "w"
crontab = "0 0 * * * *"
"#,
        )
        .unwrap();
        assert!(file.jobworkerp.is_empty());
        assert_eq!(file.schedulers.unwrap().len(), 1);
    }

    #[test]
    fn legacy_mode_leaves_out_disabled_listeners_and_schedulers() {
        let toml = format!(
            "{SERVER}
[[schedulers]]
name = \"on\"
jobworkerp = \"server1\"
workflow_url = \"a.yml\"
crontab = \"0 0 * * * *\"

[[schedulers]]
name = \"off\"
jobworkerp = \"server1\"
workflow_url = \"b.yml\"
crontab = \"0 0 * * * *\"
enabled = false

[[listeners]]
name = \"off-listener\"
jobworkerp = \"server1\"
listen_jobworkerp = \"server1\"
listen_worker_name = \"w\"
workflow_url = \"c.yml\"
enabled = false
"
        );
        let file = WorkflowSettingFile::parse_for_legacy(&toml).unwrap();
        let names: Vec<_> = file
            .schedulers
            .unwrap()
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(names, ["on"]);
        assert!(file.listeners.unwrap().is_empty());
        assert_eq!(file.jobworkerp.len(), 1);
    }
}
