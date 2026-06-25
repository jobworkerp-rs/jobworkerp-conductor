pub mod config_management;
pub mod cron_scheduler;
pub mod execution_ref;
pub mod jobworkerp_server;
pub mod module;
pub mod notification;
pub(in crate::infra) mod resource;
pub mod slack_event_handler;
pub mod worker_result_handler;

use crate::error::UiEventHandlerError;
use anyhow::Result;
use command_utils::util::id_generator::{self, IDGenerator};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct IdGeneratorWrapper {
    id_generator: Arc<Mutex<IDGenerator>>,
}

impl Default for IdGeneratorWrapper {
    fn default() -> Self {
        Self::new()
    }
}

impl IdGeneratorWrapper {
    pub fn new() -> Self {
        IdGeneratorWrapper {
            id_generator: Arc::new(Mutex::new(id_generator::new_generator_by_ip())),
        }
    }
    // thread safe
    pub fn generate_id(&self) -> Result<i64> {
        self.id_generator
            .lock()
            .map_err(|e| UiEventHandlerError::GenerateIdError(e.to_string()).into())
            .and_then(|mut g| g.generate())
    }
}

pub trait UseIdGenerator {
    fn id_generator(&self) -> &IdGeneratorWrapper;
}
