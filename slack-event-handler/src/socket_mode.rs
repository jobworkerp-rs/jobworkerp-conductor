// Socket Mode Integration
// Manages Slack Socket Mode connection and event dispatch

use crate::event_matcher::EventMatcher;
use crate::handler_executor::HandlerExecutor;
use anyhow::{Context, Result};
use slack_morphism::prelude::*;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Socket Mode Listener State
/// Holds event matcher and handler executor for callback processing
#[derive(Clone)]
pub struct SocketModeState {
    event_matcher: Arc<EventMatcher>,
    handler_executor: Arc<HandlerExecutor>,
}

impl SocketModeState {
    pub fn new(event_matcher: Arc<EventMatcher>, handler_executor: Arc<HandlerExecutor>) -> Self {
        Self {
            event_matcher,
            handler_executor,
        }
    }
}

/// Socket Mode Listener
/// Connects to Slack via Socket Mode and dispatches events to handlers
pub struct SocketModeListener {
    app_token: SlackApiToken,
    event_matcher: Arc<EventMatcher>,
    handler_executor: Arc<HandlerExecutor>,
}

/// Push events handler function
async fn push_events_handler(
    event: SlackPushEventCallback,
    _client: Arc<SlackHyperClient>,
    state_storage: SlackClientEventsUserState,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let state_storage_read = state_storage.read().await;
    let state = state_storage_read
        .get_user_state::<SocketModeState>()
        .ok_or_else(|| anyhow::anyhow!("Failed to get SocketModeState from user state"))?;

    let event_matcher = state.event_matcher.clone();
    let handler_executor = state.handler_executor.clone();

    tokio::spawn(async move {
        SocketModeListener::handle_push_event(event.event, event_matcher, handler_executor).await;
    });

    // Return OK to acknowledge the event to Slack
    Ok(())
}

impl SocketModeListener {
    /// Create new Socket Mode listener
    pub fn new(
        app_token: String,
        event_matcher: Arc<EventMatcher>,
        handler_executor: Arc<HandlerExecutor>,
    ) -> Result<Self> {
        let app_token = SlackApiToken::new(app_token.into());

        Ok(Self {
            app_token,
            event_matcher,
            handler_executor,
        })
    }

    /// Start Socket Mode listener with shutdown signal
    /// Returns when shutdown signal is received or connection fails
    pub async fn start(self: Arc<Self>, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        tracing::info!("Starting Slack Socket Mode listener");

        // Initialize rustls CryptoProvider if not already set
        // This is required for rustls 0.23+ to avoid panic
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::ring::default_provider().install_default();
            tracing::debug!("Installed rustls ring CryptoProvider");
        }

        // Create Slack client
        let client = Arc::new(SlackClient::new(SlackClientHyperConnector::new()?));

        // Create state for callbacks
        let state = SocketModeState::new(self.event_matcher.clone(), self.handler_executor.clone());

        // Create Socket Mode callbacks
        let socket_mode_callbacks =
            SlackSocketModeListenerCallbacks::new().with_push_events(push_events_handler);

        let listener_environment = Arc::new(
            SlackClientEventsListenerEnvironment::new(client.clone())
                .with_user_state(state)
                .with_error_handler(|err, _client, _state| {
                    tracing::error!("Socket Mode error: {:?}", err);
                    // Return OK to acknowledge the event to Slack
                    HttpStatusCode::OK
                }),
        );

        let socket_mode_listener = SlackClientSocketModeListener::new(
            &SlackClientSocketModeConfig::new(),
            listener_environment.clone(),
            socket_mode_callbacks,
        );

        // Start listening with automatic reconnection
        let listen_future = socket_mode_listener.listen_for(&self.app_token);

        // Wait for either listener completion or shutdown signal
        tokio::select! {
            result = listen_future => {
                match result {
                    Ok(_) => {
                        // After listen_for completes, call serve() to start the event loop
                        tracing::info!("Socket Mode connection established, starting event loop");
                        let serve_result = socket_mode_listener.serve().await;
                        tracing::info!("Socket Mode listener completed with status: {}", serve_result);
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!("Socket Mode listener failed: {:?}", e);
                        Err(anyhow::anyhow!("Socket Mode listener error: {:?}", e))
                    }
                }
            }
            _ = &mut shutdown_rx => {
                tracing::info!("Socket Mode listener received shutdown signal");
                Ok(())
            }
        }
    }

    /// Handle push event from Slack
    async fn handle_push_event(
        event: SlackEventCallbackBody,
        event_matcher: Arc<EventMatcher>,
        handler_executor: Arc<HandlerExecutor>,
    ) {
        // Extract channel ID from event
        let channel_id = Self::extract_channel_id(&event);

        tracing::debug!(
            "Received Slack event: type={:?}, channel={:?}",
            Self::event_type_name(&event),
            channel_id
        );

        // Match event to handlers
        let channel_id_obj = SlackChannelId::new(channel_id.clone());
        let matched_handlers = match event_matcher.match_event_to_handlers(&event, &channel_id_obj)
        {
            Ok(handlers) => handlers,
            Err(e) => {
                tracing::error!("Failed to match event to handlers: {:?}", e);
                return;
            }
        };

        if matched_handlers.is_empty() {
            tracing::debug!("No handlers matched for event");
            return;
        }

        tracing::info!(
            "Matched {} handler(s) for event: type={:?}, channel={:?}",
            matched_handlers.len(),
            Self::event_type_name(&event),
            channel_id
        );

        // Convert event to JSON payload
        let event_payload = match Self::event_to_json(&event) {
            Ok(payload) => payload,
            Err(e) => {
                tracing::error!("Failed to convert event to JSON: {:?}", e);
                return;
            }
        };

        // Execute workflows for matched handlers (in parallel)
        for handler in matched_handlers {
            let executor = handler_executor.clone();
            let handler_clone = handler.clone();
            let payload_clone = event_payload.clone();

            tokio::spawn(async move {
                let handler_name = handler_clone
                    .data
                    .as_ref()
                    .map(|d| d.name.as_str())
                    .unwrap_or("unknown");
                let handler_id = handler_clone.id.as_ref().map(|id| id.value);

                tracing::info!(
                    "Executing workflow for handler '{}' (id={:?})",
                    handler_name,
                    handler_id
                );

                if let Err(e) = executor
                    .execute_workflow_with_retry(&handler_clone, payload_clone)
                    .await
                {
                    tracing::error!(
                        "Workflow execution failed for handler '{}' (id={:?}): {:?}",
                        handler_name,
                        handler_id,
                        e
                    );
                }
            });
        }
    }

    /// Extract channel ID from event
    fn extract_channel_id(event: &SlackEventCallbackBody) -> String {
        match event {
            SlackEventCallbackBody::Message(msg) => msg
                .origin
                .channel
                .as_ref()
                .map(|c| c.to_string())
                .unwrap_or_default(),
            SlackEventCallbackBody::AppMention(mention) => mention.channel.to_string(),
            SlackEventCallbackBody::ReactionAdded(reaction) => {
                if let SlackReactionsItem::Message(msg) = &reaction.item {
                    msg.origin
                        .channel
                        .as_ref()
                        .map(|c| c.to_string())
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            }
            SlackEventCallbackBody::ReactionRemoved(reaction) => {
                if let SlackReactionsItem::Message(msg) = &reaction.item {
                    msg.origin
                        .channel
                        .as_ref()
                        .map(|c| c.to_string())
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        }
    }

    /// Get event type name for logging
    fn event_type_name(event: &SlackEventCallbackBody) -> &'static str {
        match event {
            SlackEventCallbackBody::Message(_) => "message",
            SlackEventCallbackBody::AppMention(_) => "app_mention",
            SlackEventCallbackBody::ReactionAdded(_) => "reaction_added",
            SlackEventCallbackBody::ReactionRemoved(_) => "reaction_removed",
            _ => "other",
        }
    }

    /// Convert event to JSON payload
    fn event_to_json(event: &SlackEventCallbackBody) -> Result<serde_json::Value> {
        serde_json::to_value(event).context("Failed to serialize event to JSON")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_name() {
        // This is a basic compile test
        // Actual event handling tests will be in integration tests
        let msg = SlackMessageEvent::new(
            SlackMessageOrigin::new(SlackTs::new("1234567890.123456".to_string()))
                .with_channel(SlackChannelId::new("C123456".to_string())),
            SlackMessageSender::new().with_user(SlackUserId::new("U123456".to_string())),
        );
        assert_eq!(
            SocketModeListener::event_type_name(&SlackEventCallbackBody::Message(msg)),
            "message"
        );
    }

    #[test]
    fn test_extract_channel_id() {
        let msg = SlackMessageEvent::new(
            SlackMessageOrigin::new(SlackTs::new("1234567890.123456".to_string()))
                .with_channel(SlackChannelId::new("C123456".to_string())),
            SlackMessageSender::new().with_user(SlackUserId::new("U123456".to_string())),
        );

        let channel_id =
            SocketModeListener::extract_channel_id(&SlackEventCallbackBody::Message(msg));
        assert_eq!(channel_id, "C123456");
    }
}
