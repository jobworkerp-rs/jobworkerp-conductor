use proto::jobworkerp_conductor::data::{
    JobworkerpServer, JobworkerpServerData, JobworkerpServerId,
};

// db row definitions
#[derive(sqlx::FromRow)]
pub struct JobworkerpServerRow {
    pub id: i64,
    pub name: String,
    pub host: String,
    pub port: String,
    pub ssl_enabled: bool,
    pub description: Option<String>,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

impl JobworkerpServerRow {
    pub fn to_proto(&self) -> JobworkerpServer {
        JobworkerpServer {
            id: Some(JobworkerpServerId { value: self.id }),
            data: Some(JobworkerpServerData {
                name: self.name.clone(),
                host: self.host.clone(),
                port: self.port.clone(),
                ssl_enabled: self.ssl_enabled,
                description: self.description.clone(),
                enabled: self.enabled,
                created_at: self.created_at,
                updated_at: self.updated_at,
            }),
        }
    }
}
