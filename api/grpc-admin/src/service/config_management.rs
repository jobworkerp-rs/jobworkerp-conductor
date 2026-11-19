use debug_stub_derive::DebugStub;
use std::{fmt::Debug, sync::Arc};

use crate::proto::jobworkerp_conductor::service::config_management_service_server::ConfigManagementService;
use crate::proto::jobworkerp_conductor::service::{
    ExportTomlRequest, ExportTomlResponse, ImportTomlRequest, ImportTomlResponse, SuccessResponse,
};
use crate::service::error_handle::handle_error;
use app::app::config_management::{
    ConfigManagementApp, ConfigManagementAppImpl, ImportReport, TomlExport,
};
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
        import_response(
            self.app()
                .import_toml_config(&req.toml_content, req.overwrite_existing, req.validate_only)
                .await,
            req.validate_only,
        )
    }

    #[tracing::instrument]
    async fn export_toml(
        &self,
        request: tonic::Request<ExportTomlRequest>,
    ) -> Result<tonic::Response<ExportTomlResponse>, tonic::Status> {
        let _span = Self::trace_request("config_management", "export_toml", &request);
        let req = request.get_ref();

        Ok(Response::new(export_response(
            self.app().export_toml_config(req.enabled_only).await,
        )))
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
        // Always a dry run, whatever validate_only says.
        import_response(
            self.app()
                .import_toml_config(&req.toml_content, req.overwrite_existing, true)
                .await,
            true,
        )
    }
}

/// Entry problems come back as `validation_errors` with `is_success` false;
/// only a failure to run the import at all is a gRPC error.
fn import_response(
    result: anyhow::Result<ImportReport>,
    dry_run: bool,
) -> Result<tonic::Response<ImportTomlResponse>, tonic::Status> {
    let report = result.map_err(|e| handle_error(&e))?;
    let counts = format!(
        "{} servers, {} schedulers, {} listeners, {} Slack handlers",
        report.servers, report.schedulers, report.listeners, report.slack_handlers
    );
    let is_success = report.errors.is_empty();
    let message = match (is_success, dry_run) {
        (true, true) => format!("Valid: would import {counts}"),
        (true, false) => format!("Imported {counts}"),
        (false, _) => format!(
            "{} entries cannot be imported; nothing was written",
            report.errors.len()
        ),
    };
    Ok(Response::new(ImportTomlResponse {
        is_success,
        message,
        imported_servers: report.servers,
        imported_schedulers: report.schedulers,
        imported_listeners: report.listeners,
        imported_slack_handlers: report.slack_handlers,
        validation_errors: report.errors,
    }))
}

/// A file with entries left out is still a usable export, so it succeeds and
/// `left_out` lists what is missing from it.
fn export_response(result: anyhow::Result<TomlExport>) -> ExportTomlResponse {
    match result {
        Ok(TomlExport {
            toml_content,
            left_out,
        }) => {
            let message = if left_out.is_empty() {
                "Export successful".to_string()
            } else {
                format!(
                    "Exported; {} entries left out: {}",
                    left_out.len(),
                    left_out.join("; ")
                )
            };
            ExportTomlResponse {
                is_success: true,
                message,
                toml_content,
                left_out,
            }
        }
        Err(e) => {
            let message = format!("Export failed: {e}");
            tracing::error!("{}", message);
            ExportTomlResponse {
                is_success: false,
                message,
                toml_content: String::new(),
                left_out: vec![],
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

#[cfg(test)]
mod tests {
    use super::*;

    fn report(counts: [i32; 4], errors: &[&str]) -> ImportReport {
        let [servers, schedulers, listeners, slack_handlers] = counts;
        ImportReport {
            servers,
            schedulers,
            listeners,
            slack_handlers,
            errors: errors.iter().map(|e| e.to_string()).collect(),
        }
    }

    fn counts(r: &ImportTomlResponse) -> [i32; 4] {
        [
            r.imported_servers,
            r.imported_schedulers,
            r.imported_listeners,
            r.imported_slack_handlers,
        ]
    }

    #[test]
    fn a_successful_import_reports_its_counts() {
        let response = import_response(Ok(report([1, 2, 3, 4], &[])), false)
            .unwrap()
            .into_inner();
        assert!(response.is_success);
        assert_eq!(
            response.message,
            "Imported 1 servers, 2 schedulers, 3 listeners, 4 Slack handlers"
        );
        assert_eq!(counts(&response), [1, 2, 3, 4]);
        let dry_run = import_response(Ok(report([1, 2, 3, 4], &[])), true)
            .unwrap()
            .into_inner();
        assert!(dry_run.message.starts_with("Valid: would import 1 servers"));
        assert_eq!(counts(&dry_run), [1, 2, 3, 4]);
    }

    #[test]
    fn a_failed_import_lists_its_errors() {
        for dry_run in [false, true] {
            let response = import_response(
                Ok(report([0, 0, 0, 0], &["Scheduler 'x': no crontab"])),
                dry_run,
            )
            .unwrap()
            .into_inner();
            assert!(!response.is_success);
            assert_eq!(
                response.message,
                "1 entries cannot be imported; nothing was written"
            );
            assert_eq!(response.validation_errors, ["Scheduler 'x': no crontab"]);
        }
    }

    #[test]
    fn an_import_that_cannot_run_is_a_grpc_error() {
        assert!(import_response(Err(anyhow::anyhow!("db down")), false).is_err());
    }

    #[test]
    fn an_export_lists_what_it_left_out() {
        let complete = export_response(Ok(TomlExport {
            toml_content: "x = 1".to_string(),
            left_out: vec![],
        }));
        assert!(complete.is_success);
        assert_eq!(complete.message, "Export successful");
        assert!(complete.left_out.is_empty());

        let missing = "listener 'a': jobworkerp server 9 does not exist";
        let partial = export_response(Ok(TomlExport {
            toml_content: "x = 1".to_string(),
            left_out: vec![missing.to_string()],
        }));
        assert!(partial.is_success);
        assert_eq!(partial.toml_content, "x = 1");
        assert_eq!(partial.left_out, [missing]);
        assert!(
            partial.message.contains("1 entries left out"),
            "{}",
            partial.message
        );

        let failed = export_response(Err(anyhow::anyhow!("db down")));
        assert!(!failed.is_success);
        assert!(failed.toml_content.is_empty());
    }
}
