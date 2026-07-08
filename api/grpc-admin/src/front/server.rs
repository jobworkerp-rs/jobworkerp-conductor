use crate::proto::jobworkerp_conductor::service::config_management_service_server::ConfigManagementServiceServer;
use crate::proto::jobworkerp_conductor::service::cron_scheduler_service_server::CronSchedulerServiceServer;
use crate::proto::jobworkerp_conductor::service::execution_status_service_server::ExecutionStatusServiceServer;
use crate::proto::jobworkerp_conductor::service::jobworkerp_server_service_server::JobworkerpServerServiceServer;
use crate::proto::jobworkerp_conductor::service::slack_event_handler_service_server::SlackEventHandlerServiceServer;
use crate::proto::jobworkerp_conductor::service::worker_result_handler_service_server::WorkerResultHandlerServiceServer;
use crate::proto::FILE_DESCRIPTOR_SET;
use crate::service::config_management::ConfigManagementGrpcImpl;
use crate::service::cron_scheduler::CronSchedulerGrpcImpl;
use crate::service::execution_status::ExecutionStatusGrpcImpl;
use crate::service::jobworkerp_server::JobworkerpServerGrpcImpl;
use crate::service::slack_event_handler::SlackEventHandlerGrpcImpl;
use crate::service::worker_result_handler::WorkerResultHandlerGrpcImpl;
use anyhow::Result;
use app::module::AppModule;
use grpc_utils::enable_grpc_web;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

pub async fn create_server(
    addr: SocketAddr,
    use_web: bool,
    max_frame_size: Option<u32>,
) -> Result<()> {
    create_server_with_shutdown(addr, use_web, max_frame_size, wait_for_ctrl_c_shutdown()).await
}

pub async fn create_server_with_shutdown<F>(
    addr: SocketAddr,
    use_web: bool,
    max_frame_size: Option<u32>,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    // reflection
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    let repository_module = infra::infra::module::RepositoryModule::new_by_env().await;
    let app_module = AppModule::new_by_env(repository_module).await;
    // TODO implement grpc server
    let worker_result_handler =
        WorkerResultHandlerGrpcImpl::new(app_module.worker_result_handler_app);
    let jobworkerp_server = JobworkerpServerGrpcImpl::new(app_module.jobworkerp_server_app);
    let cron_scheduler = CronSchedulerGrpcImpl::new(app_module.cron_scheduler_app);
    let slack_event_handler = SlackEventHandlerGrpcImpl::new(app_module.slack_event_handler_app);
    let config_management = ConfigManagementGrpcImpl::new(app_module.config_management_app);
    let execution_status = ExecutionStatusGrpcImpl::new(app_module.execution_status_app);
    if use_web {
        Server::builder()
            .accept_http1(true) // for gRPC-web
            .max_frame_size(max_frame_size) // 16MB
            // .layer(GrpcWebLayer::new()) // for grpc-web. server type is changed if this line is added
            .add_service(enable_grpc_web(WorkerResultHandlerServiceServer::new(
                worker_result_handler,
            )))
            .add_service(enable_grpc_web(JobworkerpServerServiceServer::new(
                jobworkerp_server,
            )))
            .add_service(enable_grpc_web(CronSchedulerServiceServer::new(
                cron_scheduler,
            )))
            .add_service(enable_grpc_web(SlackEventHandlerServiceServer::new(
                slack_event_handler,
            )))
            .add_service(enable_grpc_web(ConfigManagementServiceServer::new(
                config_management,
            )))
            .add_service(enable_grpc_web(ExecutionStatusServiceServer::new(
                execution_status,
            )))
            .add_service(reflection)
            .serve_with_shutdown(addr, async {
                shutdown.await;
            })
            .await
            .map_err(|e| e.into())
    } else {
        Server::builder()
            .max_frame_size(max_frame_size) // 16MB
            .add_service(reflection)
            .add_service(WorkerResultHandlerServiceServer::new(worker_result_handler))
            .add_service(JobworkerpServerServiceServer::new(jobworkerp_server))
            .add_service(CronSchedulerServiceServer::new(cron_scheduler))
            .add_service(SlackEventHandlerServiceServer::new(slack_event_handler))
            .add_service(ConfigManagementServiceServer::new(config_management))
            .add_service(ExecutionStatusServiceServer::new(execution_status))
            .serve_with_shutdown(addr, async {
                shutdown.await;
            })
            .await
            .map_err(|e| e.into())
    }
}

/// 共有AppModuleを使用してgRPCサーバーを作成
///
/// NotificationServiceのインスタンスを統一し、確実な通知配信を実現するため、
/// 外部から提供されたAppModuleを使用してgRPCサービスを作成する。
pub async fn create_server_with_app_module(
    app_module: Arc<AppModule>,
    addr: SocketAddr,
    use_web: bool,
    max_frame_size: Option<u32>,
) -> Result<()> {
    create_server_with_app_module_and_shutdown(
        app_module,
        addr,
        use_web,
        max_frame_size,
        wait_for_ctrl_c_shutdown(),
    )
    .await
}

pub async fn create_server_with_app_module_and_shutdown<F>(
    app_module: Arc<AppModule>,
    addr: SocketAddr,
    use_web: bool,
    max_frame_size: Option<u32>,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tracing::info!(
        "✅ 共有AppModuleを使用してgRPCサーバーを作成 (addr: {}, web: {})",
        addr,
        use_web
    );

    // reflection
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    // 🚀 重要: 共有AppModuleを使用（新規作成ではない）
    tracing::info!("🔗 共有AppModuleからgRPCサービスを作成");
    let worker_result_handler =
        WorkerResultHandlerGrpcImpl::new(app_module.worker_result_handler_app.clone());
    let jobworkerp_server = JobworkerpServerGrpcImpl::new(app_module.jobworkerp_server_app.clone());
    let cron_scheduler = CronSchedulerGrpcImpl::new(app_module.cron_scheduler_app.clone());
    let slack_event_handler =
        SlackEventHandlerGrpcImpl::new(app_module.slack_event_handler_app.clone());
    let config_management = ConfigManagementGrpcImpl::new(app_module.config_management_app.clone());
    let execution_status = ExecutionStatusGrpcImpl::new(app_module.execution_status_app.clone());

    tracing::info!("📡 同一NotificationServiceを使用することを確認済み");

    if use_web {
        Server::builder()
            .accept_http1(true) // for gRPC-web
            .max_frame_size(max_frame_size) // 16MB
            // .layer(GrpcWebLayer::new()) // for grpc-web. server type is changed if this line is added
            .add_service(enable_grpc_web(WorkerResultHandlerServiceServer::new(
                worker_result_handler,
            )))
            .add_service(enable_grpc_web(JobworkerpServerServiceServer::new(
                jobworkerp_server,
            )))
            .add_service(enable_grpc_web(CronSchedulerServiceServer::new(
                cron_scheduler,
            )))
            .add_service(enable_grpc_web(SlackEventHandlerServiceServer::new(
                slack_event_handler,
            )))
            .add_service(enable_grpc_web(ConfigManagementServiceServer::new(
                config_management,
            )))
            .add_service(enable_grpc_web(ExecutionStatusServiceServer::new(
                execution_status,
            )))
            .add_service(reflection)
            .serve_with_shutdown(addr, async {
                shutdown.await;
            })
            .await
            .map_err(|e| e.into())
    } else {
        Server::builder()
            .max_frame_size(max_frame_size) // 16MB
            .add_service(reflection)
            .add_service(WorkerResultHandlerServiceServer::new(worker_result_handler))
            .add_service(JobworkerpServerServiceServer::new(jobworkerp_server))
            .add_service(CronSchedulerServiceServer::new(cron_scheduler))
            .add_service(SlackEventHandlerServiceServer::new(slack_event_handler))
            .add_service(ConfigManagementServiceServer::new(config_management))
            .add_service(ExecutionStatusServiceServer::new(execution_status))
            .serve_with_shutdown(addr, async {
                shutdown.await;
            })
            .await
            .map_err(|e| e.into())
    }
}

async fn wait_for_ctrl_c_shutdown() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            tracing::info!("received ctrl_c");
        }
        Err(e) => tracing::error!("failed to listen for ctrl_c: {:?}", e),
    }
}
