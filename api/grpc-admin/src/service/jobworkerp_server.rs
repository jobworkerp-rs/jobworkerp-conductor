use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::proto::jobworkerp_conductor::service::jobworkerp_server_service_server::JobworkerpServerService;
use crate::proto::jobworkerp_conductor::service::{
    CountResponse, CreateJobworkerpServerResponse, FindByNameRequest, FindCondition,
    FindListRequest, OptionalJobworkerpServerResponse, SuccessResponse,
};
use crate::service::error_handle::handle_error;
use app::app::jobworkerp_server::{JobworkerpServerApp, JobworkerpServerAppImpl};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use proto::jobworkerp_conductor::data::{
    JobworkerpServer, JobworkerpServerData, JobworkerpServerId,
};
use tonic::Response;

pub trait JobworkerpServerGrpc {
    fn app(&self) -> &JobworkerpServerAppImpl;
}

const DEFAULT_TTL: Duration = Duration::from_secs(30);
const LIST_TTL: Duration = Duration::from_secs(5);

#[tonic::async_trait]
impl<T: JobworkerpServerGrpc + Tracing + Send + Debug + Sync + 'static> JobworkerpServerService
    for T
{
    #[tracing::instrument]
    async fn create(
        &self,
        request: tonic::Request<JobworkerpServerData>,
    ) -> Result<tonic::Response<CreateJobworkerpServerResponse>, tonic::Status> {
        let _span = Self::trace_request("jobworkerp_server", "create", &request);
        let req = request.get_ref();
        match self.app().create_jobworkerp_server(req).await {
            Ok(id) => Ok(Response::new(CreateJobworkerpServerResponse {
                id: Some(id),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn update(
        &self,
        request: tonic::Request<JobworkerpServer>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("jobworkerp_server", "update", &request);
        let req = request.get_ref();
        if let Some(i) = &req.id {
            match self.app().update_jobworkerp_server(i, &req.data).await {
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
        request: tonic::Request<JobworkerpServerId>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("jobworkerp_server", "delete", &request);
        let req = request.get_ref();
        match self.app().delete_jobworkerp_server(req).await {
            Ok(r) => Ok(Response::new(SuccessResponse { is_success: r })),
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn find(
        &self,
        request: tonic::Request<JobworkerpServerId>,
    ) -> Result<tonic::Response<OptionalJobworkerpServerResponse>, tonic::Status> {
        let _s = Self::trace_request("jobworkerp_server", "find", &request);
        let req = request.get_ref();
        match self
            .app()
            .find_jobworkerp_server(req, Some(&DEFAULT_TTL))
            .await
        {
            Ok(res) => Ok(Response::new(OptionalJobworkerpServerResponse {
                data: res,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_by_name(
        &self,
        request: tonic::Request<FindByNameRequest>,
    ) -> Result<tonic::Response<OptionalJobworkerpServerResponse>, tonic::Status> {
        let _s = Self::trace_request("jobworkerp_server", "find_by_name", &request);
        let req = request.get_ref();
        match self
            .app()
            .find_jobworkerp_server_by_name(&req.name, Some(&DEFAULT_TTL))
            .await
        {
            Ok(res) => Ok(Response::new(OptionalJobworkerpServerResponse {
                data: res,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindListStream = BoxStream<'static, Result<JobworkerpServer, tonic::Status>>;
    #[tracing::instrument]
    async fn find_list(
        &self,
        request: tonic::Request<FindListRequest>,
    ) -> Result<tonic::Response<Self::FindListStream>, tonic::Status> {
        let _s = Self::trace_request("jobworkerp_server", "find_list", &request);
        let req = request.get_ref();
        let ttl = if req.limit.is_some() {
            LIST_TTL
        } else {
            DEFAULT_TTL
        };
        match self
            .app()
            .find_jobworkerp_server_list(req.limit.as_ref(), req.offset.as_ref(), Some(&ttl))
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
        let _s = Self::trace_request("jobworkerp_server", "count", &request);
        match self.app().count().await {
            Ok(res) => Ok(Response::new(CountResponse { total: res })),
            Err(e) => Err(handle_error(&e)),
        }
    }
}

#[derive(DebugStub)]
pub(crate) struct JobworkerpServerGrpcImpl {
    #[debug_stub = "JobworkerpServerAppImpl"]
    jobworkerp_server_app: Arc<JobworkerpServerAppImpl>,
}

impl JobworkerpServerGrpcImpl {
    pub fn new(jobworkerp_server_app: Arc<JobworkerpServerAppImpl>) -> Self {
        JobworkerpServerGrpcImpl {
            jobworkerp_server_app,
        }
    }
}
impl JobworkerpServerGrpc for JobworkerpServerGrpcImpl {
    fn app(&self) -> &JobworkerpServerAppImpl {
        &self.jobworkerp_server_app
    }
}

// use tracing
impl Tracing for JobworkerpServerGrpcImpl {}
