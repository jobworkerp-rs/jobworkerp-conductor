pub mod callbacks;

use anyhow::Result;
use callbacks::SocketModeCallbacksImpl;
use callbacks::SocketModeState;
use slack_morphism::hyper_tokio::SlackClientHyperConnector;
use slack_morphism::prelude::*;
use std::sync::Arc;

// TODO call from ui handler(improve setting integration)
pub struct SlackMessageHandlerServer {
    app_token: Arc<SlackApiToken>,
    socket_mode_listener: SlackClientSocketModeListener<SlackClientHyperHttpsConnector>,
}
impl SlackMessageHandlerServer {
    pub async fn new(app_token: Arc<SlackApiToken>) -> Result<Self> {
        let client = Arc::new(SlackClient::new(SlackClientHyperConnector::new()?));

        let state = SocketModeState::new_by_env().await?;
        let socket_mode_callbacks = SocketModeCallbacksImpl::new().callbacks();

        let listener_environment = Arc::new(
            SlackClientEventsListenerEnvironment::new(client.clone())
                .with_user_state(state)
                .with_error_handler(|err, _client, _states| {
                    tracing::warn!("{:#?}", err);

                    // This return value should be OK if we want to return successful ack to the Slack server using Web-sockets
                    // https://api.slack.com/apis/connections/socket-implement#acknowledge
                    // so that Slack knows whether to retry
                    HttpStatusCode::OK
                }),
        );

        let socket_mode_listener = SlackClientSocketModeListener::new(
            &SlackClientSocketModeConfig::new(),
            listener_environment,
            socket_mode_callbacks,
        );

        Ok(Self {
            app_token: app_token.clone(),
            socket_mode_listener,
        })
    }
    pub async fn serve(&self) -> Result<()> {
        self.socket_mode_listener
            .listen_for(&self.app_token)
            .await?;
        let st = self.socket_mode_listener.serve().await;
        tracing::info!("server exit. status: {}", st);
        Ok(())
    }
}
