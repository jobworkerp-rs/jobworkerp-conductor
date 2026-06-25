// LocalConfigStore extensions for jobworkerp-conductor
// Provides InitialConfig loading capabilities

use shared::LocalConfigStore;

use crate::initialization::InitialConfig;

/// Extension trait for LocalConfigStore to support InitialConfig loading
pub trait LocalConfigStoreExt {
    fn from_initial_config(initial_config: InitialConfig) -> Self;
    fn load_from_initial_config(&mut self, initial_config: InitialConfig);
}

impl LocalConfigStoreExt for LocalConfigStore {
    fn from_initial_config(initial_config: InitialConfig) -> Self {
        let mut store = Self::new();
        store.load_from_initial_config(initial_config);
        store
    }

    fn load_from_initial_config(&mut self, initial_config: InitialConfig) {
        tracing::info!("Loading initial configuration into local store");

        for scheduler in initial_config.cron_schedulers {
            if let Err(e) = self.upsert_cron_scheduler(scheduler) {
                tracing::error!("Failed to insert cron scheduler: {}", e);
            }
        }

        for handler in initial_config.worker_result_handlers {
            if let Err(e) = self.upsert_worker_result_handler(handler) {
                tracing::error!("Failed to insert worker result handler: {}", e);
            }
        }

        for server in initial_config.jobworkerp_servers {
            if let Err(e) = self.upsert_jobworkerp_server(server) {
                tracing::error!("Failed to insert jobworkerp server: {}", e);
            }
        }

        for slack_handler in initial_config.slack_event_handlers {
            if let Err(e) = self.upsert_slack_event_handler(slack_handler) {
                tracing::error!("Failed to insert slack event handler: {}", e);
            }
        }

        let stats = self.get_stats();

        tracing::info!(
            "Initial configuration loaded: {} cron_schedulers, {} worker_result_handlers, {} jobworkerp_servers, {} slack_event_handlers",
            stats.cron_scheduler_count,
            stats.worker_result_handler_count,
            stats.jobworkerp_server_count,
            stats.slack_event_handler_count
        );
    }
}
