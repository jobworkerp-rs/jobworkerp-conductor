use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::proto::jobworkerp_conductor::service::worker_result_handler_service_server::WorkerResultHandlerService;
use crate::proto::jobworkerp_conductor::service::{
    CountResponse, CreateWorkerResultHandlerResponse, FindByNameRequest, FindCondition,
    FindListRequest, OptionalWorkerResultHandlerResponse, SuccessResponse,
};
use crate::service::error_handle::handle_error;
use app::app::worker_result_handler::{WorkerResultHandlerApp, WorkerResultHandlerAppImpl};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use proto::jobworkerp_conductor::data::{
    WorkerResultHandler, WorkerResultHandlerData, WorkerResultHandlerId,
};
use shared::validation::validate_args;
use tonic::Response;

shared::define_validate_execution_target!(
    WorkerResultHandlerData,
    proto::jobworkerp_conductor::data::worker_result_handler_data
);

pub trait WorkerResultHandlerGrpc {
    fn app(&self) -> &WorkerResultHandlerAppImpl;
}

const DEFAULT_TTL: Duration = Duration::from_secs(30);
const LIST_TTL: Duration = Duration::from_secs(5);

#[tonic::async_trait]
impl<T: WorkerResultHandlerGrpc + Tracing + Send + Debug + Sync + 'static>
    WorkerResultHandlerService for T
{
    #[tracing::instrument]
    async fn create(
        &self,
        request: tonic::Request<WorkerResultHandlerData>,
    ) -> Result<tonic::Response<CreateWorkerResultHandlerResponse>, tonic::Status> {
        let _span = Self::trace_request("worker_result_handler", "create", &request);
        let req = request.get_ref();

        // 引数バリデーション（gRPCレイヤーで実施）
        validate_args(&req.args)
            .map_err(|e| tonic::Status::invalid_argument(format!("Invalid args: {}", e)))?;
        validate_execution_target(req)?;

        match self.app().create_worker_result_handler(req).await {
            Ok(id) => Ok(Response::new(CreateWorkerResultHandlerResponse {
                id: Some(id),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn update(
        &self,
        request: tonic::Request<WorkerResultHandler>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("worker_result_handler", "update", &request);
        let req = request.get_ref();
        if let Some(i) = &req.id {
            // 引数バリデーション（gRPCレイヤーで実施）
            if let Some(data) = &req.data {
                validate_args(&data.args)
                    .map_err(|e| tonic::Status::invalid_argument(format!("Invalid args: {}", e)))?;
                validate_execution_target(data)?;
            }

            match self.app().update_worker_result_handler(i, &req.data).await {
                Ok(res) => Ok(Response::new(SuccessResponse { is_success: res })),
                Err(e) => Err(handle_error(&e)),
            }
        } else {
            tracing::warn!("id not found in updating: {:?}", req);
            Err(tonic::Status::not_found("id not found".to_string()))
        }
    }
    #[tracing::instrument]
    async fn delete(
        &self,
        request: tonic::Request<WorkerResultHandlerId>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("worker_result_handler", "delete", &request);
        let req = request.get_ref();
        match self.app().delete_worker_result_handler(req).await {
            Ok(r) => Ok(Response::new(SuccessResponse { is_success: r })),
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn find(
        &self,
        request: tonic::Request<WorkerResultHandlerId>,
    ) -> Result<tonic::Response<OptionalWorkerResultHandlerResponse>, tonic::Status> {
        let _s = Self::trace_request("worker_result_handler", "find", &request);
        let req = request.get_ref();
        match self
            .app()
            .find_worker_result_handler(req, Some(&DEFAULT_TTL))
            .await
        {
            Ok(res) => Ok(Response::new(OptionalWorkerResultHandlerResponse {
                data: res,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_by_name(
        &self,
        request: tonic::Request<FindByNameRequest>,
    ) -> Result<tonic::Response<OptionalWorkerResultHandlerResponse>, tonic::Status> {
        let _s = Self::trace_request("worker_result_handler", "find_by_name", &request);
        let req = request.get_ref();
        match self
            .app()
            .find_worker_result_handler_by_name(&req.name, Some(&DEFAULT_TTL))
            .await
        {
            Ok(res) => Ok(Response::new(OptionalWorkerResultHandlerResponse {
                data: res,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindListStream = BoxStream<'static, Result<WorkerResultHandler, tonic::Status>>;
    #[tracing::instrument]
    async fn find_list(
        &self,
        request: tonic::Request<FindListRequest>,
    ) -> Result<tonic::Response<Self::FindListStream>, tonic::Status> {
        let _s = Self::trace_request("worker_result_handler", "find_list", &request);
        let req = request.get_ref();
        let ttl = if req.limit.is_some() {
            LIST_TTL
        } else {
            DEFAULT_TTL
        };
        match self
            .app()
            .find_worker_result_handler_list(req.limit.as_ref(), req.offset.as_ref(), Some(&ttl))
            .await
        {
            Ok(list) => {
                // TODO streamingのより良いやり方がないか?
                Ok(Response::new(Box::pin(stream! {
                    for s in list {
                        yield Ok(s)
                    }
                })))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn count(
        &self,
        request: tonic::Request<FindCondition>,
    ) -> Result<tonic::Response<CountResponse>, tonic::Status> {
        let _s = Self::trace_request("worker_result_handler", "count", &request);
        match self.app().count().await {
            Ok(res) => Ok(Response::new(CountResponse { total: res })),
            Err(e) => Err(handle_error(&e)),
        }
    }
}

#[derive(DebugStub)]
pub(crate) struct WorkerResultHandlerGrpcImpl {
    #[debug_stub = "WorkerResultHandlerAppImpl"]
    worker_result_handler_app: Arc<WorkerResultHandlerAppImpl>,
}

impl WorkerResultHandlerGrpcImpl {
    pub fn new(worker_result_handler_app: Arc<WorkerResultHandlerAppImpl>) -> Self {
        WorkerResultHandlerGrpcImpl {
            worker_result_handler_app,
        }
    }
}
impl WorkerResultHandlerGrpc for WorkerResultHandlerGrpcImpl {
    fn app(&self) -> &WorkerResultHandlerAppImpl {
        &self.worker_result_handler_app
    }
}

// use tracing
impl Tracing for WorkerResultHandlerGrpcImpl {}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::jobworkerp_conductor::data::worker_result_handler_data::ExecutionTarget;
    use proto::jobworkerp_conductor::data::{WorkerExecution, WorkflowExecution};

    fn make_data(
        workflow_url: &str,
        execution_target: Option<ExecutionTarget>,
    ) -> WorkerResultHandlerData {
        WorkerResultHandlerData {
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
