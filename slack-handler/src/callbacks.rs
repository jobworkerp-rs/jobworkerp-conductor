// use async_trait::async_trait;
use anyhow::{anyhow, Context, Result};
use jobworkerp_client::client::helper::UseJobworkerpClientHelper;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_client::jobworkerp::data::{
    QueueType, ResponseType, RetryPolicy, RetryType, WorkerData,
};
use jobworkerp_slack::protobuf::SlackMessageRequest;
use prost::Message;
use slack_morphism::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

// #[async_trait]
// trait SocketModeCallbacks: Send + Sync + 'static {
//     // Botが送信したInteractive Componentsに対するAction
//     async fn interaction_events_function(
//         event: SlackInteractionEvent,
//         _client: Arc<SlackHyperClient>,
//         _states: SlackClientEventsUserState,
//     ) -> Result<(), Box<dyn std::error::Error + Sync + Send>>
//     where
//         Self: Send + Sync + 'static;

//     async fn command_events_function<'a>(
//         event: SlackCommandEvent,
//         client: Arc<SlackHyperClient>,
//         _states: SlackClientEventsUserState,
//     ) -> Result<SlackCommandEventResponse, Box<dyn std::error::Error + Sync + Send>>;

//     async fn push_events_sm_function<'a>(
//         event: SlackPushEventCallback,
//         client: Arc<SlackHyperClient>,
//         _states: SlackClientEventsUserState,
//     ) -> Result<(), Box<dyn std::error::Error + Sync + Send>>;
// }

#[derive(Clone)]
pub struct SocketModeState {
    pub api_token: Arc<SlackApiToken>,
    pub jobworkerp_client: Arc<JobworkerpClientWrapper>,
    pub jobworkerp_runner_name: String,
}
impl SocketModeState {
    const REQUEST_TIMEOUT_SEC: u32 = 10;
    const JOB_TIMEOUT_SEC: u32 = 3600;
    pub async fn new_by_env() -> Result<Self> {
        let token_value: SlackApiTokenValue = std::env::var("SLACK_BOT_TOKEN")
            .context("parse env SLACK_BOT_TOKEN")?
            .into();
        let api_token = Arc::new(SlackApiToken::new(token_value));
        let jobworkerp_runner_name =
            std::env::var("SLACK_RUNNER_NAME").context("parse env SLACK_RUNNER_NAME")?;

        let jobworkerp_client =
            Arc::new(JobworkerpClientWrapper::new_by_env(Some(Self::REQUEST_TIMEOUT_SEC)).await?);
        Ok(Self {
            api_token,
            jobworkerp_client,
            jobworkerp_runner_name,
        })
    }
}

#[derive(Clone)]
pub struct SocketModeCallbacksImpl {}
impl SocketModeCallbacksImpl {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_socket_mode_state(
        state_storage: &SlackClientEventsUserState,
    ) -> Result<SocketModeState> {
        let state_storage = state_storage.read().await;
        state_storage
            .get_user_state::<SocketModeState>()
            .cloned()
            .ok_or(anyhow!("get_user_state"))
    }

    pub async fn get_jobworkerp_client(
        state_storage: &SlackClientEventsUserState,
    ) -> Result<Arc<JobworkerpClientWrapper>> {
        let state_storage = state_storage.read().await;
        let cli = state_storage
            .get_user_state::<SocketModeState>()
            .ok_or(anyhow!("get_user_state"))?
            .jobworkerp_client
            .clone();
        Ok(cli)
    }

    pub async fn get_jobworkerp_runner_name(
        state_storage: &SlackClientEventsUserState,
    ) -> Result<String> {
        let state_storage = state_storage.read().await;
        let name = state_storage
            .get_user_state::<SocketModeState>()
            .ok_or(anyhow!("get_user_state"))?
            .jobworkerp_runner_name
            .clone();
        Ok(name)
    }

    pub fn callbacks(&self) -> SlackSocketModeListenerCallbacks<SlackClientHyperHttpsConnector> {
        // Botが送信したInteractive Componentsに対するAction
        async fn interaction_events_function(
            event: SlackInteractionEvent,
            _client: Arc<SlackHyperClient>,
            _states: SlackClientEventsUserState,
        ) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
            tracing::info!("Interaction event: {:#?}", event);
            Ok(())
        }
        async fn push_events_sm_function(
            event: SlackPushEventCallback,
            _client: Arc<SlackHyperClient>,
            state_storage: SlackClientEventsUserState,
        ) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
            match event.event {
                //  メッセージはとりあえずworkerになげる
                SlackEventCallbackBody::Message(message_event) => {
                    // TODO process content.text or content.
                    if let Some(content) = message_event.content {
                        if let Some(txt) = content.text {
                            tracing::info!("message text: {:#?}", txt);
                            let arg = SlackMessageRequest { message: txt };
                            let mut buf = Vec::with_capacity(arg.encoded_len());
                            arg.encode(&mut buf).unwrap();
                            let client =
                                SocketModeCallbacksImpl::get_jobworkerp_client(&state_storage)
                                    .await?;
                            // enqueue only
                            let runner_name =
                                SocketModeCallbacksImpl::get_jobworkerp_runner_name(&state_storage)
                                    .await?;
                            let jr = client
                                .enqueue_worker_job(
                                    None,
                                    Arc::new(HashMap::new()),
                                    &SocketModeCallbacksImpl::get_slack_message_worker_data(
                                        client.clone(),
                                        runner_name.as_str(),
                                    )
                                    .await?,
                                    buf,
                                    SocketModeState::JOB_TIMEOUT_SEC,
                                    None,
                                    None,
                                )
                                .await
                                .context("handle_message")?;
                            if jr.id.is_none() {
                                tracing::error!("enqueue_worker_job failed");
                                Err(anyhow!("enqueue_worker_job failed"))?;
                            }
                        }
                        if let Some(attachments) = content.attachments {
                            tracing::info!("message attachments: {:#?}", attachments);
                        }
                    }
                    Ok(())
                }
                _ => {
                    tracing::info!("Push event: {:#?}", event);
                    Ok(())
                }
            }
        }
        SlackSocketModeListenerCallbacks::new()
            //.with_command_events(command_events_function)
            .with_interaction_events(interaction_events_function)
            .with_push_events(push_events_sm_function)
    }

    async fn get_slack_message_worker_data(
        client: Arc<JobworkerpClientWrapper>,
        runner_name: &str,
    ) -> Result<WorkerData> {
        let runner_id = client
            .find_runner_by_name(None, Arc::new(HashMap::new()), runner_name)
            .await?
            .and_then(|x| x.id)
            .ok_or(anyhow!("runner not found"))?;

        Ok(WorkerData {
            name: "SlackMessageHandleWorker".to_string(),
            description: "Slack message handle worker".to_string(),
            runner_id: Some(runner_id),
            runner_settings: vec![], // empty
            retry_policy: Some(RetryPolicy {
                r#type: RetryType::Exponential as i32,
                interval: 1000,
                max_interval: 30000,
                max_retry: 3,
                basis: 2.0,
            }),
            periodic_interval: 0,
            channel: None, // use default
            queue_type: QueueType::Normal as i32,
            response_type: ResponseType::NoResult as i32, // no result
            store_success: false,
            store_failure: false,
            use_static: false,
            broadcast_results: true,
        })
    }
}

impl Default for SocketModeCallbacksImpl {
    fn default() -> Self {
        Self::new()
    }
}
