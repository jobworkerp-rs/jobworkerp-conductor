use debug_stub_derive::DebugStub;
use std::{fmt::Debug, sync::Arc};

use crate::proto::jobworkerp_conductor::service::config_management_service_server::ConfigManagementService;
use crate::proto::jobworkerp_conductor::service::{
    ExportTomlRequest, ExportTomlResponse, ImportTomlRequest, ImportTomlResponse, SuccessResponse,
};
use crate::service::error_handle::handle_error;
use app::app::config_management::{ConfigManagementApp, ConfigManagementAppImpl};
use command_utils::trace::Tracing;
use tonic::Response;

pub trait ConfigManagementGrpc {
    fn app(&self) -> &ConfigManagementAppImpl;
}

#[tonic::async_trait]
impl<T: ConfigManagementGrpc + Tracing + Send + Debug + Sync + 'static> ConfigManagementService
    for T
{
    #[tracing::instrument]
    async fn import_toml(
        &self,
        request: tonic::Request<ImportTomlRequest>,
    ) -> Result<tonic::Response<ImportTomlResponse>, tonic::Status> {
        let _span = Self::trace_request("config_management", "import_toml", &request);
        let req = request.get_ref();

        match self
            .app()
            .import_toml_config(&req.toml_content, req.overwrite_existing)
            .await
        {
            Ok((servers, schedulers, listeners)) => Ok(Response::new(ImportTomlResponse {
                is_success: true,
                message: format!(
                    "Imported {servers} servers, {schedulers} schedulers, {listeners} listeners"
                ),
                imported_servers: servers,
                imported_schedulers: schedulers,
                imported_listeners: listeners,
                validation_errors: vec![],
            })),
            Err(e) => {
                let error_msg = format!("Import failed: {e}");
                tracing::error!("{}", error_msg);
                Ok(Response::new(ImportTomlResponse {
                    is_success: false,
                    message: error_msg,
                    imported_servers: 0,
                    imported_schedulers: 0,
                    imported_listeners: 0,
                    validation_errors: vec![e.to_string()],
                }))
            }
        }
    }

    #[tracing::instrument]
    async fn export_toml(
        &self,
        request: tonic::Request<ExportTomlRequest>,
    ) -> Result<tonic::Response<ExportTomlResponse>, tonic::Status> {
        let _span = Self::trace_request("config_management", "export_toml", &request);
        let req = request.get_ref();

        match self.app().export_toml_config(req.enabled_only).await {
            Ok(toml_content) => Ok(Response::new(ExportTomlResponse {
                is_success: true,
                message: "Export successful".to_string(),
                toml_content,
            })),
            Err(e) => {
                let error_msg = format!("Export failed: {e}");
                tracing::error!("{}", error_msg);
                Ok(Response::new(ExportTomlResponse {
                    is_success: false,
                    message: error_msg,
                    toml_content: String::new(),
                }))
            }
        }
    }

    #[tracing::instrument]
    async fn clear_all_configs(
        &self,
        request: tonic::Request<()>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _span = Self::trace_request("config_management", "clear_all_configs", &request);

        match self.app().clear_all_configs().await {
            Ok(_) => Ok(Response::new(SuccessResponse { is_success: true })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn validate_config(
        &self,
        request: tonic::Request<ImportTomlRequest>,
    ) -> Result<tonic::Response<ImportTomlResponse>, tonic::Status> {
        let _span = Self::trace_request("config_management", "validate_config", &request);
        let req = request.get_ref();

        match self.app().validate_toml_config(&req.toml_content).await {
            Ok(_) => Ok(Response::new(ImportTomlResponse {
                is_success: true,
                message: "Configuration is valid".to_string(),
                imported_servers: 0,
                imported_schedulers: 0,
                imported_listeners: 0,
                validation_errors: vec![],
            })),
            Err(e) => {
                let error_msg = format!("Validation failed: {e}");
                tracing::error!("{}", error_msg);
                Ok(Response::new(ImportTomlResponse {
                    is_success: false,
                    message: error_msg,
                    imported_servers: 0,
                    imported_schedulers: 0,
                    imported_listeners: 0,
                    validation_errors: vec![e.to_string()],
                }))
            }
        }
    }
}

#[derive(DebugStub)]
pub(crate) struct ConfigManagementGrpcImpl {
    #[debug_stub = "ConfigManagementAppImpl"]
    config_management_app: Arc<ConfigManagementAppImpl>,
}

impl ConfigManagementGrpcImpl {
    pub fn new(config_management_app: Arc<ConfigManagementAppImpl>) -> Self {
        ConfigManagementGrpcImpl {
            config_management_app,
        }
    }
}

impl ConfigManagementGrpc for ConfigManagementGrpcImpl {
    fn app(&self) -> &ConfigManagementAppImpl {
        &self.config_management_app
    }
}

// use tracing
impl Tracing for ConfigManagementGrpcImpl {}
