use proto::jobworkerp_conductor::data::{
    ExecutionRef, ExecutionRefId, ExecutionSourceType, JobworkerpServerId,
};

#[derive(sqlx::FromRow)]
pub struct ExecutionRefRow {
    pub id: i64,
    pub source_type: i32,
    pub source_id: i64,
    pub source_name: String,
    pub jobworkerp_server_id: i64,
    pub job_id: Option<i64>,
    pub triggered_at: i64,
    pub trigger_context_json: Option<String>,
    pub enqueue_error: Option<String>,
    pub created_at: i64,
    pub result_status: Option<i32>,
}

impl ExecutionRefRow {
    pub fn to_proto(&self) -> ExecutionRef {
        ExecutionRef {
            id: Some(ExecutionRefId { value: self.id }),
            source_type: ExecutionSourceType::try_from(self.source_type)
                .unwrap_or(ExecutionSourceType::Unspecified)
                .into(),
            source_id: self.source_id,
            source_name: self.source_name.clone(),
            jobworkerp_server_id: Some(JobworkerpServerId {
                value: self.jobworkerp_server_id,
            }),
            job_id: self.job_id,
            triggered_at: self.triggered_at,
            trigger_context_json: self.trigger_context_json.clone(),
            enqueue_error: self.enqueue_error.clone(),
            created_at: self.created_at,
            result_status: self.result_status,
        }
    }
}
