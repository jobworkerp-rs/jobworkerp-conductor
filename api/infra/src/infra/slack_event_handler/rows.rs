use proto::jobworkerp_conductor::data::{
    slack_event_handler_data::ExecutionTarget, JobworkerpServerId, ReactionOperation,
    SlackEventHandler, SlackEventHandlerData, SlackEventHandlerId, WorkerExecution,
    WorkflowExecution,
};

// Database row definitions for slack_event_handler table
#[derive(sqlx::FromRow)]
pub struct SlackEventHandlerRow {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,

    // Common event conditions
    pub slack_channel_id: Option<String>,

    // Message event conditions
    pub message_pattern: Option<String>,
    pub mention_required: bool,

    // Reaction event conditions
    pub reaction_names: Option<String>,
    pub reaction_operation: Option<String>,
    pub reaction_user_filter: Option<String>,

    // Workflow execution settings
    pub jobworkerp_server_id: i64,
    pub workflow_url: String,
    pub channel: Option<String>,
    pub timeout_sec: i32,
    pub args: Option<String>,
    pub worker_name: Option<String>,
    pub using: Option<String>,

    // Metadata
    pub created_at: i64,
    pub updated_at: i64,
}

impl SlackEventHandlerRow {
    /// Convert DB string to ReactionOperation enum
    fn reaction_operation_from_db(value: Option<&String>) -> i32 {
        value.map_or(ReactionOperation::Unspecified as i32, |s| {
            match s.as_str() {
                "added" => ReactionOperation::Added as i32,
                "removed" => ReactionOperation::Removed as i32,
                "both" => ReactionOperation::Both as i32,
                _ => {
                    tracing::warn!("Unknown reaction_operation value in DB: '{}'", s);
                    ReactionOperation::Unspecified as i32
                }
            }
        })
    }

    pub fn to_proto(&self) -> SlackEventHandler {
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
            // This should not occur in normal operation (gRPC validation prevents it),
            // but can happen if the DB is modified directly. The handler will fail at
            // execution time with "No execution target specified".
            None
        };

        SlackEventHandler {
            id: Some(SlackEventHandlerId { value: self.id }),
            data: Some(SlackEventHandlerData {
                name: self.name.clone(),
                description: self.description.clone().unwrap_or_default(),
                enabled: self.enabled,
                slack_channel_id: self.slack_channel_id.clone(),
                message_pattern: self.message_pattern.clone(),
                mention_required: self.mention_required,
                reaction_names: self.reaction_names.clone(),
                reaction_operation: Self::reaction_operation_from_db(
                    self.reaction_operation.as_ref(),
                ),
                reaction_user_filter: self.reaction_user_filter.clone(),
                jobworkerp_server_id: Some(JobworkerpServerId {
                    value: self.jobworkerp_server_id,
                }),
                // Backward compat: populate deprecated fields 11/12 for old clients
                workflow_url: self.workflow_url.clone(),
                channel: self.channel.clone().unwrap_or_default(),
                timeout_sec: Some(self.timeout_sec),
                args: self.args.clone(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                execution_target,
            }),
        }
    }
}

/// Convert ReactionOperation enum to DB string
pub fn reaction_operation_to_db(op: i32) -> Option<String> {
    ReactionOperation::try_from(op).ok().and_then(|enum_val| {
        match enum_val {
            ReactionOperation::Added => Some("added".to_string()),
            ReactionOperation::Removed => Some("removed".to_string()),
            ReactionOperation::Both => Some("both".to_string()),
            ReactionOperation::Unspecified => None, // Store as NULL in DB
        }
    })
}
