//! Conversions between the settings file (TOML) items and the stored data.
//! Export and import both go through here, so a setting read back from an
//! exported file is the one that was written. Errors leave out the item's
//! name; callers add it with the kind of item.

use anyhow::{anyhow, Context, Result};
use infra::infra::slack_event_handler::rows::{
    reaction_operation_from_str, reaction_operation_to_db,
};
use jobworkerp_handler::settings::{
    JobWorkerpSetting, SlackHandlerSettingItem, WorkflowSettingItem,
};
use proto::jobworkerp_conductor::data::{
    cron_scheduler_data, slack_event_handler_data, worker_result_handler_data, CronSchedulerData,
    JobworkerpServerData, JobworkerpServerId, ReactionOperation, SlackEventHandlerData,
    WorkerExecution, WorkerResultHandlerData, WorkflowExecution,
};
use shared::grpc_endpoint::parse_grpc_endpoint;

/// The Slack handler timeout stored when the file gives none: the column's
/// default, which an INSERT naming the column does not apply.
pub const DEFAULT_SLACK_TIMEOUT_SEC: i32 = 3600;

fn server_name(
    id: Option<&JobworkerpServerId>,
    names: &impl Fn(i64) -> Option<String>,
) -> Result<String> {
    let id = id.ok_or_else(|| anyhow!("no jobworkerp server"))?.value;
    names(id).ok_or_else(|| anyhow!("jobworkerp server {id} does not exist"))
}

fn non_empty(value: &Option<String>) -> Option<String> {
    value.as_ref().filter(|v| !v.is_empty()).cloned()
}

/// The execution target an item names, built by the kind's own constructor:
/// a worker wins over a workflow URL, as when the settings are stored.
fn target_of<T>(
    worker_name: &Option<String>,
    using: &Option<String>,
    workflow_url: &str,
    channel: &Option<String>,
    worker: impl FnOnce(WorkerExecution) -> T,
    workflow: impl FnOnce(WorkflowExecution) -> T,
) -> Result<T> {
    if let Some(worker_name) = non_empty(worker_name) {
        Ok(worker(WorkerExecution {
            worker_name,
            r#using: non_empty(using),
        }))
    } else if !workflow_url.is_empty() {
        Ok(workflow(WorkflowExecution {
            workflow_url: workflow_url.to_string(),
            channel: channel.clone(),
        }))
    } else {
        Err(anyhow!("either worker_name or workflow_url must be set"))
    }
}

/// An item's target fields from a stored target.
fn set_target(
    item: &mut WorkflowSettingItem,
    worker: Option<&WorkerExecution>,
    workflow: Option<&WorkflowExecution>,
) {
    if let Some(w) = worker {
        item.worker_name = Some(w.worker_name.clone());
        item.using = w.r#using.clone();
    }
    if let Some(w) = workflow {
        item.workflow_url = w.workflow_url.clone();
        item.channel = non_empty(&w.channel);
    }
}

pub fn server_item(data: &JobworkerpServerData) -> JobWorkerpSetting {
    let scheme = if data.ssl_enabled { "https" } else { "http" };
    JobWorkerpSetting {
        name: data.name.clone(),
        address: format!("{scheme}://{}:{}", data.host, data.port),
        description: non_empty(&data.description),
        enabled: Some(data.enabled),
    }
}

pub fn server_data(item: &JobWorkerpSetting, now: i64) -> Result<JobworkerpServerData> {
    let endpoint = parse_grpc_endpoint(&item.address)
        .with_context(|| format!("invalid address '{}'", item.address))?;
    Ok(JobworkerpServerData {
        name: item.name.clone(),
        host: endpoint.host,
        port: endpoint.port.to_string(),
        ssl_enabled: endpoint.tls,
        description: non_empty(&item.description),
        enabled: item.is_enabled(),
        created_at: now,
        updated_at: now,
    })
}

pub fn scheduler_item(
    data: &CronSchedulerData,
    names: &impl Fn(i64) -> Option<String>,
) -> Result<WorkflowSettingItem> {
    use cron_scheduler_data::ExecutionTarget as T;
    let mut item = WorkflowSettingItem {
        name: data.name.clone(),
        jobworkerp: server_name(data.jobworkerp_server_id.as_ref(), names)?,
        crontab: Some(data.crontab.clone()),
        args: data.args.clone(),
        job_timeout_sec: data.job_timeout_sec,
        description: non_empty(&data.description),
        enabled: Some(data.enabled),
        ..Default::default()
    };
    match &data.execution_target {
        Some(T::Worker(w)) => set_target(&mut item, Some(w), None),
        Some(T::Workflow(w)) => set_target(&mut item, None, Some(w)),
        None => {}
    }
    Ok(item)
}

pub fn scheduler_data(
    item: &WorkflowSettingItem,
    server: JobworkerpServerId,
    now: i64,
) -> Result<CronSchedulerData> {
    use cron_scheduler_data::ExecutionTarget as T;
    let crontab = item.crontab.clone().ok_or_else(|| anyhow!("no crontab"))?;
    let target = target_of(
        &item.worker_name,
        &item.using,
        &item.workflow_url,
        &item.channel,
        T::Worker,
        T::Workflow,
    )?;
    Ok(CronSchedulerData {
        name: item.name.clone(),
        jobworkerp_server_id: Some(server),
        crontab,
        enabled: item.is_enabled(),
        description: non_empty(&item.description),
        args: item.args.clone(),
        job_timeout_sec: item.job_timeout_sec,
        execution_target: Some(target),
        created_at: now,
        updated_at: now,
        ..Default::default()
    })
}

pub fn listener_item(
    data: &WorkerResultHandlerData,
    names: &impl Fn(i64) -> Option<String>,
) -> Result<WorkflowSettingItem> {
    use worker_result_handler_data::ExecutionTarget as T;
    let mut item = WorkflowSettingItem {
        name: data.name.clone(),
        jobworkerp: server_name(data.process_jobworkerp_server_id.as_ref(), names)?,
        listen_jobworkerp: Some(server_name(
            data.listen_jobworkerp_server_id.as_ref(),
            names,
        )?),
        listen_worker_name: Some(data.listen_worker_name.clone()),
        args: data.args.clone(),
        description: non_empty(&data.description),
        enabled: Some(data.enabled),
        ..Default::default()
    };
    match &data.execution_target {
        Some(T::Worker(w)) => set_target(&mut item, Some(w), None),
        Some(T::Workflow(w)) => set_target(&mut item, None, Some(w)),
        None => {}
    }
    Ok(item)
}

pub fn listener_data(
    item: &WorkflowSettingItem,
    listen_server: JobworkerpServerId,
    process_server: JobworkerpServerId,
    now: i64,
) -> Result<WorkerResultHandlerData> {
    use worker_result_handler_data::ExecutionTarget as T;
    let listen_worker_name =
        non_empty(&item.listen_worker_name).ok_or_else(|| anyhow!("no listen_worker_name"))?;
    // Resolved to an id by the caller; only its presence is checked here.
    non_empty(&item.listen_jobworkerp).ok_or_else(|| anyhow!("no listen_jobworkerp"))?;
    let target = target_of(
        &item.worker_name,
        &item.using,
        &item.workflow_url,
        &item.channel,
        T::Worker,
        T::Workflow,
    )?;
    Ok(WorkerResultHandlerData {
        name: item.name.clone(),
        listen_jobworkerp_server_id: Some(listen_server),
        listen_worker_name,
        process_jobworkerp_server_id: Some(process_server),
        enabled: item.is_enabled(),
        description: non_empty(&item.description),
        args: item.args.clone(),
        execution_target: Some(target),
        created_at: now,
        updated_at: now,
        ..Default::default()
    })
}

pub fn slack_item(
    data: &SlackEventHandlerData,
    names: &impl Fn(i64) -> Option<String>,
) -> Result<SlackHandlerSettingItem> {
    use slack_event_handler_data::ExecutionTarget as T;
    let (worker, workflow) = match &data.execution_target {
        Some(T::Worker(w)) => (Some(w), None),
        Some(T::Workflow(w)) => (None, Some(w)),
        None => (None, None),
    };
    Ok(SlackHandlerSettingItem {
        name: data.name.clone(),
        description: non_empty(&Some(data.description.clone())),
        enabled: Some(data.enabled),
        jobworkerp: server_name(data.jobworkerp_server_id.as_ref(), names)?,
        slack_channel_id: data.slack_channel_id.clone(),
        message_pattern: data.message_pattern.clone(),
        mention_required: data.mention_required,
        reaction_names: data.reaction_names.clone(),
        reaction_operation: reaction_operation_to_db(data.reaction_operation),
        reaction_user_filter: data.reaction_user_filter.clone(),
        workflow_url: workflow.map(|w| w.workflow_url.clone()),
        channel: workflow.and_then(|w| non_empty(&w.channel)),
        worker_name: worker.map(|w| w.worker_name.clone()),
        using: worker.and_then(|w| w.r#using.clone()),
        timeout_sec: data.timeout_sec,
        args: data.args.clone(),
    })
}

pub fn slack_data(
    item: &SlackHandlerSettingItem,
    server: JobworkerpServerId,
    now: i64,
) -> Result<SlackEventHandlerData> {
    use slack_event_handler_data::ExecutionTarget as T;
    let reaction_operation = match item.reaction_operation.as_deref() {
        None | Some("") => ReactionOperation::Unspecified,
        Some(name) => reaction_operation_from_str(name).ok_or_else(|| {
            anyhow!("reaction_operation must be added, removed or both, not '{name}'")
        })?,
    };
    let target = target_of(
        &item.worker_name,
        &item.using,
        item.workflow_url.as_deref().unwrap_or_default(),
        &item.channel,
        T::Worker,
        T::Workflow,
    )?;
    Ok(SlackEventHandlerData {
        name: item.name.clone(),
        description: item.description.clone().unwrap_or_default(),
        enabled: item.is_enabled(),
        slack_channel_id: item.slack_channel_id.clone(),
        message_pattern: item.message_pattern.clone(),
        mention_required: item.mention_required,
        reaction_names: item.reaction_names.clone(),
        reaction_operation: reaction_operation as i32,
        reaction_user_filter: item.reaction_user_filter.clone(),
        jobworkerp_server_id: Some(server),
        timeout_sec: Some(item.timeout_sec.unwrap_or(DEFAULT_SLACK_TIMEOUT_SEC)),
        args: item.args.clone(),
        execution_target: Some(target),
        created_at: now,
        updated_at: now,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::jobworkerp_conductor::data::{
        cron_scheduler_data, slack_event_handler_data, worker_result_handler_data,
        JobworkerpServerId, ReactionOperation, WorkerExecution, WorkflowExecution,
    };

    const NOW: i64 = 1_700_000_000;
    const SERVER: JobworkerpServerId = JobworkerpServerId { value: 10 };
    const OTHER: JobworkerpServerId = JobworkerpServerId { value: 20 };

    fn names(id: i64) -> Option<String> {
        match id {
            10 => Some("main".to_string()),
            20 => Some("other".to_string()),
            _ => None,
        }
    }

    #[test]
    fn server_round_trips_with_description_and_enabled() {
        let data = JobworkerpServerData {
            name: "main".to_string(),
            host: "jobworkerp.example".to_string(),
            port: "9443".to_string(),
            ssl_enabled: true,
            description: Some("primary".to_string()),
            enabled: false,
            created_at: NOW,
            updated_at: NOW,
        };
        let item = server_item(&data);
        assert_eq!(item.address, "https://jobworkerp.example:9443");
        assert_eq!(server_data(&item, NOW).unwrap(), data);
    }

    #[test]
    fn server_without_enabled_is_enabled() {
        let item = JobWorkerpSetting {
            name: "main".to_string(),
            address: "http://localhost".to_string(),
            description: None,
            enabled: None,
        };
        let data = server_data(&item, NOW).unwrap();
        assert!(data.enabled);
        assert_eq!(data.port, "80");
        assert!(server_data(
            &JobWorkerpSetting {
                address: "not a url".to_string(),
                ..item
            },
            NOW
        )
        .is_err());
        // Only gRPC over HTTP or HTTPS is reachable.
        let ftp = JobWorkerpSetting {
            name: "main".to_string(),
            address: "ftp://localhost".to_string(),
            description: None,
            enabled: None,
        };
        assert!(server_data(&ftp, NOW).is_err());
    }

    #[test]
    fn scheduler_round_trips_either_target() {
        for target in [
            cron_scheduler_data::ExecutionTarget::Worker(WorkerExecution {
                worker_name: "w".to_string(),
                r#using: Some("run".to_string()),
            }),
            cron_scheduler_data::ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "https://example.com/wf.yml".to_string(),
                channel: Some("batch".to_string()),
            }),
        ] {
            let data = CronSchedulerData {
                name: "nightly".to_string(),
                jobworkerp_server_id: Some(SERVER),
                crontab: "0 0 3 * * *".to_string(),
                enabled: false,
                description: Some("night".to_string()),
                args: Some("{}".to_string()),
                job_timeout_sec: Some(120),
                execution_target: Some(target),
                created_at: NOW,
                updated_at: NOW,
                ..Default::default()
            };
            let item = scheduler_item(&data, &names).unwrap();
            assert_eq!(item.jobworkerp, "main");
            assert_eq!(scheduler_data(&item, SERVER, NOW).unwrap(), data);
        }
    }

    #[test]
    fn scheduler_needs_a_crontab_and_a_target() {
        let item = WorkflowSettingItem {
            name: "x".to_string(),
            jobworkerp: "main".to_string(),
            crontab: Some("0 0 * * * *".to_string()),
            ..Default::default()
        };
        assert!(scheduler_data(&item, SERVER, NOW).is_err());
        let with_target = WorkflowSettingItem {
            worker_name: Some("w".to_string()),
            ..item.clone()
        };
        assert!(scheduler_data(&with_target, SERVER, NOW).is_ok());
        let no_crontab = WorkflowSettingItem {
            crontab: None,
            ..with_target
        };
        assert!(scheduler_data(&no_crontab, SERVER, NOW).is_err());
    }

    #[test]
    fn listener_round_trips_with_both_servers() {
        let data = WorkerResultHandlerData {
            name: "on-result".to_string(),
            listen_jobworkerp_server_id: Some(OTHER),
            listen_worker_name: "source".to_string(),
            process_jobworkerp_server_id: Some(SERVER),
            enabled: false,
            description: Some("follow up".to_string()),
            args: None,
            execution_target: Some(worker_result_handler_data::ExecutionTarget::Worker(
                WorkerExecution {
                    worker_name: "w".to_string(),
                    r#using: None,
                },
            )),
            created_at: NOW,
            updated_at: NOW,
            ..Default::default()
        };
        let item = listener_item(&data, &names).unwrap();
        assert_eq!(item.listen_jobworkerp.as_deref(), Some("other"));
        assert_eq!(item.jobworkerp, "main");
        assert_eq!(listener_data(&item, OTHER, SERVER, NOW).unwrap(), data);
    }

    #[test]
    fn listener_needs_both_servers_named() {
        let item = WorkflowSettingItem {
            name: "on-result".to_string(),
            jobworkerp: "main".to_string(),
            listen_worker_name: Some("source".to_string()),
            worker_name: Some("w".to_string()),
            ..Default::default()
        };
        for listen in [None, Some(String::new())] {
            let missing = WorkflowSettingItem {
                listen_jobworkerp: listen,
                ..item.clone()
            };
            let e = listener_data(&missing, OTHER, SERVER, NOW).unwrap_err();
            assert_eq!(e.to_string(), "no listen_jobworkerp");
        }
        let named = WorkflowSettingItem {
            listen_jobworkerp: Some("other".to_string()),
            ..item
        };
        assert!(listener_data(&named, OTHER, SERVER, NOW).is_ok());
    }

    #[test]
    fn slack_handler_round_trips() {
        let data = SlackEventHandlerData {
            name: "thumbs".to_string(),
            description: "reacts".to_string(),
            enabled: false,
            reaction_names: Some("+1".to_string()),
            reaction_operation: ReactionOperation::Both as i32,
            reaction_user_filter: Some("^U".to_string()),
            jobworkerp_server_id: Some(SERVER),
            timeout_sec: Some(600),
            args: Some("{}".to_string()),
            execution_target: Some(slack_event_handler_data::ExecutionTarget::Workflow(
                WorkflowExecution {
                    workflow_url: "https://example.com/wf.yml".to_string(),
                    channel: None,
                },
            )),
            created_at: NOW,
            updated_at: NOW,
            ..Default::default()
        };
        let item = slack_item(&data, &names).unwrap();
        assert_eq!(item.reaction_operation.as_deref(), Some("both"));
        assert_eq!(slack_data(&item, SERVER, NOW).unwrap(), data);
    }

    #[test]
    fn slack_handler_defaults_and_rejects_unknown_operations() {
        let item = SlackHandlerSettingItem {
            name: "deploy".to_string(),
            description: None,
            enabled: None,
            jobworkerp: "main".to_string(),
            slack_channel_id: None,
            message_pattern: Some("^deploy".to_string()),
            mention_required: false,
            reaction_names: None,
            reaction_operation: None,
            reaction_user_filter: None,
            workflow_url: None,
            channel: None,
            worker_name: Some("w".to_string()),
            using: None,
            timeout_sec: None,
            args: None,
        };
        let data = slack_data(&item, SERVER, NOW).unwrap();
        assert!(data.enabled);
        assert_eq!(data.description, "");
        // The column is NOT NULL; 3600 is its default.
        assert_eq!(data.timeout_sec, Some(3600));
        let unknown = SlackHandlerSettingItem {
            reaction_operation: Some("sometimes".to_string()),
            ..item
        };
        assert!(slack_data(&unknown, SERVER, NOW).is_err());
    }

    #[test]
    fn items_naming_an_unknown_server_are_reported() {
        let data = CronSchedulerData {
            name: "orphan".to_string(),
            jobworkerp_server_id: Some(JobworkerpServerId { value: 99 }),
            crontab: "0 0 * * * *".to_string(),
            ..Default::default()
        };
        assert!(scheduler_item(&data, &names).is_err());
    }
}
