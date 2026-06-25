use crate::proto::jobworkerp_conductor::service::execution_status_service_server::ExecutionStatusService;
use crate::proto::jobworkerp_conductor::service::{
    CountResponse, CreateExecutionRefResponse, DeleteCountResponse,
    DeleteExecutionRefsBySourceRequest, ExecutionRuntimeStatusRequest, ExecutionSourceRequest,
    FindExecutionListRequest, OptionalExecutionRefResponse, OptionalExecutionRuntimeStatusResponse,
    ReExecuteResponse, SuccessResponse, TriggerExecutionRequest, TriggerExecutionResponse,
};
use crate::service::error_handle::handle_error;
use app::app::execution_status::{DeleteResult, ExecutionStatusApp, ExecutionStatusAppImpl};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use infra::infra::execution_ref::rdb::ExecutionRefListFilter;
use proto::jobworkerp_conductor::data::{ExecutionRef, ExecutionRefId, ExecutionSourceType};
use std::fmt::Debug;
use std::sync::Arc;
use tonic::Response;

pub trait ExecutionStatusGrpc {
    fn app(&self) -> &ExecutionStatusAppImpl;
}

#[tonic::async_trait]
impl<T: ExecutionStatusGrpc + Tracing + Send + Debug + Sync + 'static> ExecutionStatusService
    for T
{
    #[tracing::instrument]
    async fn create_execution_ref(
        &self,
        request: tonic::Request<ExecutionRef>,
    ) -> Result<Response<CreateExecutionRefResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "create_execution_ref", &request);
        match self.app().create_execution_ref(request.get_ref()).await {
            Ok(id) => Ok(Response::new(CreateExecutionRefResponse { id: Some(id) })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_execution_ref(
        &self,
        request: tonic::Request<ExecutionRefId>,
    ) -> Result<Response<OptionalExecutionRefResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "find_execution_ref", &request);
        match self.app().find_execution_ref(request.get_ref()).await {
            Ok(data) => Ok(Response::new(OptionalExecutionRefResponse { data })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_latest_by_source(
        &self,
        request: tonic::Request<ExecutionSourceRequest>,
    ) -> Result<Response<OptionalExecutionRefResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "find_latest_by_source", &request);
        let req = request.get_ref();
        let source_type = ExecutionSourceType::try_from(req.source_type)
            .unwrap_or(ExecutionSourceType::Unspecified);
        match self
            .app()
            .find_latest_by_source(source_type, req.source_id)
            .await
        {
            Ok(data) => Ok(Response::new(OptionalExecutionRefResponse { data })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindListBySourceStream = BoxStream<'static, Result<ExecutionRef, tonic::Status>>;

    #[tracing::instrument]
    async fn find_list_by_source(
        &self,
        request: tonic::Request<ExecutionSourceRequest>,
    ) -> Result<Response<Self::FindListBySourceStream>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "find_list_by_source", &request);
        let req = request.get_ref();
        let source_type = ExecutionSourceType::try_from(req.source_type)
            .unwrap_or(ExecutionSourceType::Unspecified);
        match self
            .app()
            .find_list_by_source(
                source_type,
                req.source_id,
                req.limit.as_ref(),
                req.offset.as_ref(),
            )
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for item in list {
                    yield Ok(item);
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_runtime_status(
        &self,
        request: tonic::Request<ExecutionRuntimeStatusRequest>,
    ) -> Result<Response<OptionalExecutionRuntimeStatusResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "find_runtime_status", &request);
        let id = request
            .get_ref()
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("id is required"))?;
        match self.app().find_runtime_status(id).await {
            Ok(data) => Ok(Response::new(OptionalExecutionRuntimeStatusResponse {
                data,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_latest_runtime_status_by_source(
        &self,
        request: tonic::Request<ExecutionSourceRequest>,
    ) -> Result<Response<OptionalExecutionRuntimeStatusResponse>, tonic::Status> {
        let _s = Self::trace_request(
            "execution_status",
            "find_latest_runtime_status_by_source",
            &request,
        );
        let req = request.get_ref();
        let source_type = ExecutionSourceType::try_from(req.source_type)
            .unwrap_or(ExecutionSourceType::Unspecified);
        match self
            .app()
            .find_latest_runtime_status_by_source(source_type, req.source_id)
            .await
        {
            Ok(data) => Ok(Response::new(OptionalExecutionRuntimeStatusResponse {
                data,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn cancel_execution(
        &self,
        request: tonic::Request<ExecutionRefId>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "cancel_execution", &request);
        match self.app().cancel_execution(request.get_ref()).await {
            Ok(is_success) => Ok(Response::new(SuccessResponse { is_success })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn delete_execution_ref(
        &self,
        request: tonic::Request<ExecutionRefId>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "delete_execution_ref", &request);
        match self.app().delete_execution_ref(request.get_ref()).await {
            Ok(DeleteResult::Deleted) => Ok(Response::new(SuccessResponse { is_success: true })),
            // Missing is reported as a non-fatal failure (idempotent delete), matching the proto doc.
            Ok(DeleteResult::NotFound) => Ok(Response::new(SuccessResponse { is_success: false })),
            // An active / status-indeterminate ref is protected from deletion.
            Ok(DeleteResult::NotTerminal) => Err(tonic::Status::failed_precondition(
                "execution_ref is not in a terminal state; refusing to delete",
            )),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn delete_execution_refs_by_source(
        &self,
        request: tonic::Request<DeleteExecutionRefsBySourceRequest>,
    ) -> Result<Response<DeleteCountResponse>, tonic::Status> {
        let _s = Self::trace_request(
            "execution_status",
            "delete_execution_refs_by_source",
            &request,
        );
        let req = request.get_ref();
        let source_type = ExecutionSourceType::try_from(req.source_type)
            .unwrap_or(ExecutionSourceType::Unspecified);
        match self
            .app()
            .delete_execution_refs_by_source(
                source_type,
                req.source_id,
                req.include_active.unwrap_or(false),
            )
            .await
        {
            Ok(deleted) => Ok(Response::new(DeleteCountResponse {
                deleted: deleted as i64,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindListStream = BoxStream<'static, Result<ExecutionRef, tonic::Status>>;

    #[tracing::instrument]
    async fn find_list(
        &self,
        request: tonic::Request<FindExecutionListRequest>,
    ) -> Result<Response<Self::FindListStream>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "find_list", &request);
        let req = request.get_ref();
        let filter = filter_from_request(req);
        match self.app().find_list(filter, req.limit, req.offset).await {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for item in list {
                    yield Ok(item);
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn count_list(
        &self,
        request: tonic::Request<FindExecutionListRequest>,
    ) -> Result<Response<CountResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "count_list", &request);
        let filter = filter_from_request(request.get_ref());
        match self.app().count_list(filter).await {
            Ok(total) => Ok(Response::new(CountResponse { total })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn trigger_execution(
        &self,
        request: tonic::Request<TriggerExecutionRequest>,
    ) -> Result<Response<TriggerExecutionResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "trigger_execution", &request);
        let req = request.get_ref();
        let source_type = ExecutionSourceType::try_from(req.source_type)
            .unwrap_or(ExecutionSourceType::Unspecified);
        // handle_error maps the resolver's Unimplemented / NotFound to the matching gRPC status.
        match self
            .app()
            .trigger_execution(source_type, req.source_id, req.args_json.clone())
            .await
        {
            Ok(outcome) => Ok(Response::new(TriggerExecutionResponse {
                execution_ref_id: outcome.execution_ref_id,
                status: Some(outcome.status),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn re_execute(
        &self,
        request: tonic::Request<ExecutionRefId>,
    ) -> Result<Response<ReExecuteResponse>, tonic::Status> {
        let _s = Self::trace_request("execution_status", "re_execute", &request);
        // handle_error maps NotFound / Unimplemented / FailedPrecondition to the matching status.
        match self.app().re_execute(request.get_ref()).await {
            Ok(outcome) => Ok(Response::new(ReExecuteResponse {
                execution_ref_id: outcome.execution_ref_id,
                status: Some(outcome.status),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }
}

/// Map the optional proto filter fields onto the repository filter. `source_type` is kept only when
/// it denotes a real source (Unspecified means "no filter"); `jobworkerp_server_id` is flattened to
/// its inner i64.
fn filter_from_request(req: &FindExecutionListRequest) -> ExecutionRefListFilter {
    ExecutionRefListFilter {
        source_type: req.source_type.filter(|st| {
            ExecutionSourceType::try_from(*st)
                .map(|t| t != ExecutionSourceType::Unspecified)
                .unwrap_or(false)
        }),
        jobworkerp_server_id: req.jobworkerp_server_id.as_ref().map(|s| s.value),
        triggered_after: req.triggered_after,
        triggered_before: req.triggered_before,
    }
}

#[derive(DebugStub)]
pub(crate) struct ExecutionStatusGrpcImpl {
    #[debug_stub = "ExecutionStatusAppImpl"]
    execution_status_app: Arc<ExecutionStatusAppImpl>,
}

impl ExecutionStatusGrpcImpl {
    pub fn new(execution_status_app: Arc<ExecutionStatusAppImpl>) -> Self {
        Self {
            execution_status_app,
        }
    }
}

impl ExecutionStatusGrpc for ExecutionStatusGrpcImpl {
    fn app(&self) -> &ExecutionStatusAppImpl {
        &self.execution_status_app
    }
}

impl Tracing for ExecutionStatusGrpcImpl {}
