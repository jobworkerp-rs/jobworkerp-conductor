use super::config_management::rdb::ConfigManagementRepositoryImpl;
use super::cron_scheduler::rdb::CronSchedulerRepositoryImpl;
use super::execution_ref::rdb::ExecutionRefRepositoryImpl;
use super::jobworkerp_server::rdb::JobworkerpServerRepositoryImpl;
use super::slack_event_handler::rdb::SlackEventHandlerRepositoryImpl;
use super::worker_result_handler::rdb::WorkerResultHandlerRepositoryImpl;
use super::IdGeneratorWrapper;

// module for DI
// TODO repositoryをArcでラップするかは要検討
pub struct RepositoryModule {
    pub worker_result_handler_repository: WorkerResultHandlerRepositoryImpl,
    pub jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
    pub cron_scheduler_repository: CronSchedulerRepositoryImpl,
    pub slack_event_handler_repository: SlackEventHandlerRepositoryImpl,
    pub config_management_repository: ConfigManagementRepositoryImpl,
    pub execution_ref_repository: ExecutionRefRepositoryImpl,
}

impl RepositoryModule {
    pub async fn new_by_env() -> Self {
        let id_generator = IdGeneratorWrapper::new();
        let pool = super::resource::setup_rdb_by_env().await;

        let jobworkerp_server_repository =
            JobworkerpServerRepositoryImpl::new(id_generator.clone(), pool);

        RepositoryModule {
            worker_result_handler_repository: WorkerResultHandlerRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),
            jobworkerp_server_repository,
            cron_scheduler_repository: CronSchedulerRepositoryImpl::new(id_generator.clone(), pool),
            slack_event_handler_repository: SlackEventHandlerRepositoryImpl::new(
                id_generator.clone(),
                pool.clone(),
            ),
            config_management_repository: ConfigManagementRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),
            execution_ref_repository: ExecutionRefRepositoryImpl::new(id_generator.clone(), pool),
        }
    }
}
