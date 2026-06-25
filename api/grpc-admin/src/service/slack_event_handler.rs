use std::{fmt::Debug, time::Duration};

use crate::proto::jobworkerp_conductor::service::slack_event_handler_service_server::SlackEventHandlerService;
use crate::proto::jobworkerp_conductor::service::{
    CountResponse, CreateSlackEventHandlerResponse, FindByNameRequest, FindCondition,
    FindListRequest, OptionalSlackEventHandlerResponse, SuccessResponse,
};
use crate::service::error_handle::handle_error;
use app::app::slack_event_handler::{SlackEventHandlerApp, SlackEventHandlerAppImpl};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use proto::jobworkerp_conductor::data::{
    SlackEventHandler, SlackEventHandlerData, SlackEventHandlerId,
};
use shared::validation::validate_args;
use std::sync::Arc;
use tonic::Response;

shared::define_validate_execution_target!(
    SlackEventHandlerData,
    proto::jobworkerp_conductor::data::slack_event_handler_data
);

pub trait SlackEventHandlerGrpc {
    fn app(&self) -> &SlackEventHandlerAppImpl;
}

const DEFAULT_TTL: Duration = Duration::from_secs(30);
#[allow(dead_code)]
const LIST_TTL: Duration = Duration::from_secs(5);

#[tonic::async_trait]
impl<T: SlackEventHandlerGrpc + Tracing + Send + Debug + Sync + 'static> SlackEventHandlerService
    for T
{
    #[tracing::instrument]
    async fn create(
        &self,
        request: tonic::Request<SlackEventHandlerData>,
    ) -> Result<tonic::Response<CreateSlackEventHandlerResponse>, tonic::Status> {
        let _span = Self::trace_request("slack_event_handler", "create", &request);
        let req = request.get_ref();

        // 引数バリデーション（gRPCレイヤーで実施）
        validate_args(&req.args)
            .map_err(|e| tonic::Status::invalid_argument(format!("Invalid args: {}", e)))?;
        validate_execution_target(req)?;

        match self.app().create_slack_event_handler(req).await {
            Ok(id) => Ok(Response::new(CreateSlackEventHandlerResponse {
                id: Some(id),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn update(
        &self,
        request: tonic::Request<SlackEventHandler>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("slack_event_handler", "update", &request);
        let req = request.get_ref();
        if let Some(id) = &req.id {
            // 引数バリデーション（gRPCレイヤーで実施）
            if let Some(data) = &req.data {
                validate_args(&data.args)
                    .map_err(|e| tonic::Status::invalid_argument(format!("Invalid args: {}", e)))?;
                validate_execution_target(data)?;

                match self.app().update_slack_event_handler(id, data).await {
                    Ok(res) => Ok(Response::new(SuccessResponse { is_success: res })),
                    Err(e) => Err(handle_error(&e)),
                }
            } else {
                tracing::warn!("data not found in updating: {:?}", req);
                Err(tonic::Status::not_found("data not found".to_string()))
            }
        } else {
            tracing::warn!("id not found in updating: {:?}", req);
            Err(tonic::Status::not_found("id not found".to_string()))
        }
    }

    #[tracing::instrument]
    async fn delete(
        &self,
        request: tonic::Request<SlackEventHandlerId>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("slack_event_handler", "delete", &request);
        let req = request.get_ref();
        match self.app().delete_slack_event_handler(req).await {
            Ok(r) => Ok(Response::new(SuccessResponse { is_success: r })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find(
        &self,
        request: tonic::Request<SlackEventHandlerId>,
    ) -> Result<tonic::Response<OptionalSlackEventHandlerResponse>, tonic::Status> {
        let _s = Self::trace_request("slack_event_handler", "find", &request);
        let req = request.get_ref();
        match self
            .app()
            .find_slack_event_handler(req, Some(&DEFAULT_TTL))
            .await
        {
            Ok(res) => Ok(Response::new(OptionalSlackEventHandlerResponse {
                data: res,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_by_name(
        &self,
        request: tonic::Request<FindByNameRequest>,
    ) -> Result<tonic::Response<OptionalSlackEventHandlerResponse>, tonic::Status> {
        let _s = Self::trace_request("slack_event_handler", "find_by_name", &request);
        let req = request.get_ref();
        match self
            .app()
            .find_slack_event_handler_by_name(&req.name, Some(&DEFAULT_TTL))
            .await
        {
            Ok(res) => Ok(Response::new(OptionalSlackEventHandlerResponse {
                data: res,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindListStream = BoxStream<'static, Result<SlackEventHandler, tonic::Status>>;

    #[tracing::instrument]
    async fn find_list(
        &self,
        request: tonic::Request<FindListRequest>,
    ) -> Result<tonic::Response<Self::FindListStream>, tonic::Status> {
        let _s = Self::trace_request("slack_event_handler", "find_list", &request);
        let app = self.app();
        let limit = request.get_ref().limit;
        let offset = request.get_ref().offset;

        let result = match app.find_slack_event_handler_list().await {
            Ok(res) => res,
            Err(e) => return Err(handle_error(&e)),
        };

        let stream = stream! {
            let start = offset.unwrap_or(0) as usize;
            let end = if let Some(l) = limit {
                std::cmp::min(start + l as usize, result.len())
            } else {
                result.len()
            };

            for item in result.into_iter().skip(start).take(end - start) {
                yield Ok(item);
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    type FindListByConditionStream = BoxStream<'static, Result<SlackEventHandler, tonic::Status>>;

    #[tracing::instrument]
    async fn find_list_by_condition(
        &self,
        request: tonic::Request<FindCondition>,
    ) -> Result<tonic::Response<Self::FindListByConditionStream>, tonic::Status> {
        let _s = Self::trace_request("slack_event_handler", "find_list_by_condition", &request);
        let app = self.app();

        let result = match app.find_slack_event_handler_list().await {
            Ok(res) => res,
            Err(e) => return Err(handle_error(&e)),
        };

        let stream = stream! {
            for item in result {
                yield Ok(item);
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument]
    async fn count(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<CountResponse>, tonic::Status> {
        let _s = Self::trace_request("slack_event_handler", "count", &_request);
        match self.app().count().await {
            Ok(total) => Ok(Response::new(CountResponse { total })),
            Err(e) => Err(handle_error(&e)),
        }
    }
}

pub struct SlackEventHandlerGrpcImpl {
    app: Arc<SlackEventHandlerAppImpl>,
}

impl std::fmt::Debug for SlackEventHandlerGrpcImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlackEventHandlerGrpcImpl").finish()
    }
}

impl SlackEventHandlerGrpcImpl {
    pub fn new(app: Arc<SlackEventHandlerAppImpl>) -> Self {
        Self { app }
    }
}

impl SlackEventHandlerGrpc for SlackEventHandlerGrpcImpl {
    fn app(&self) -> &SlackEventHandlerAppImpl {
        &self.app
    }
}

impl Tracing for SlackEventHandlerGrpcImpl {}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::jobworkerp_conductor::data::slack_event_handler_data::ExecutionTarget;
    use proto::jobworkerp_conductor::data::{WorkerExecution, WorkflowExecution};

    fn make_data(
        workflow_url: &str,
        execution_target: Option<ExecutionTarget>,
    ) -> SlackEventHandlerData {
        SlackEventHandlerData {
            name: "test".to_string(),
            workflow_url: workflow_url.to_string(),
            execution_target,
            ..Default::default()
        }
    }

    #[test]
    fn test_workflow_execution_ok() {
        let data = make_data(
            "",
            Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "https://example.com/wf.yml".to_string(),
                channel: None,
            })),
        );
        assert!(validate_execution_target(&data).is_ok());
    }

    #[test]
    fn test_worker_execution_ok() {
        let data = make_data(
            "",
            Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: "my-worker".to_string(),
                r#using: Some("run".to_string()),
            })),
        );
        assert!(validate_execution_target(&data).is_ok());
    }

    #[test]
    fn test_legacy_fallback_ok() {
        let data = make_data("https://example.com/wf.yml", None);
        assert!(validate_execution_target(&data).is_ok());
    }

    #[test]
    fn test_execution_target_with_deprecated_field_ok() {
        // Both set: execution_target takes precedence (warn logged but OK)
        let data = make_data(
            "https://old.example.com/wf.yml",
            Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "https://new.example.com/wf.yml".to_string(),
                channel: None,
            })),
        );
        assert!(validate_execution_target(&data).is_ok());
    }

    #[test]
    fn test_nothing_set_error() {
        let data = make_data("", None);
        assert!(validate_execution_target(&data).is_err());
    }

    #[test]
    fn test_workflow_url_empty_error() {
        let data = make_data(
            "",
            Some(ExecutionTarget::Workflow(WorkflowExecution {
                workflow_url: "".to_string(),
                channel: None,
            })),
        );
        assert!(validate_execution_target(&data).is_err());
    }

    #[test]
    fn test_worker_name_empty_error() {
        let data = make_data(
            "",
            Some(ExecutionTarget::Worker(WorkerExecution {
                worker_name: "".to_string(),
                r#using: None,
            })),
        );
        assert!(validate_execution_target(&data).is_err());
    }
}
