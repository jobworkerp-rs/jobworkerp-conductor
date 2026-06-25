use proto::jobworkerp_conductor::data::{
    worker_result_handler_data::ExecutionTarget, WorkerExecution, WorkerResultHandler,
    WorkerResultHandlerData, WorkerResultHandlerId, WorkflowExecution,
};

// db row definitions
#[derive(sqlx::FromRow)]
pub struct WorkerResultHandlerRow {
    pub id: i64,
    pub name: String,
    pub listen_jobworkerp_server_id: i64,
    pub listen_worker_name: String,
    pub process_jobworkerp_server_id: i64,
    pub workflow_url: String,
    pub channel: Option<String>,
    pub enabled: bool,
    pub description: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub args: Option<String>,
    pub worker_name: Option<String>,
    pub using: Option<String>,
}

impl WorkerResultHandlerRow {
    pub fn to_proto(&self) -> WorkerResultHandler {
        // Build oneof execution_target from DB columns
        let execution_target = if self.worker_name.as_ref().is_some_and(|n| !n.is_empty()) {
            Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: self.worker_name.clone().unwrap_or_default(),
                using: self.using.clone(),
            }))
        } else if !self.workflow_url.is_empty() {
            Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: self.workflow_url.clone(),
                channel: self.channel.clone(),
            }))
        } else {
            // Both workflow_url and worker_name are empty/NULL.
            // Should not occur in normal operation; see slack_event_handler/rows.rs for details.
            None
        };

        WorkerResultHandler {
            id: Some(WorkerResultHandlerId { value: self.id }),
            data: Some(WorkerResultHandlerData {
                name: self.name.clone(),
                listen_jobworkerp_server_id: Some(
                    proto::jobworkerp_conductor::data::JobworkerpServerId {
                        value: self.listen_jobworkerp_server_id,
                    },
                ),
                listen_worker_name: self.listen_worker_name.clone(),
                process_jobworkerp_server_id: Some(
                    proto::jobworkerp_conductor::data::JobworkerpServerId {
                        value: self.process_jobworkerp_server_id,
                    },
                ),
                // Backward compat: populate deprecated fields 5/6 for old clients
                workflow_url: self.workflow_url.clone(),
                channel: self.channel.clone(),
                enabled: self.enabled,
                description: self.description.clone(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                args: self.args.clone(),
                execution_target,
            }),
        }
    }
}
