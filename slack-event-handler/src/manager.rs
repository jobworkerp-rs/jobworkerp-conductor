// Dynamic Slack Handler Manager
// Manages Slack event handlers dynamically with configuration updates

use crate::config_cache::SlackHandlerCache;
use anyhow::{Context, Result};
use proto::jobworkerp_conductor::data::SlackEventHandler;
use shared::config_events_proto::ConfigChangeEventWrapper;
use shared::SharedLocalConfigStore;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Dynamic Slack Handler Manager
/// Manages Slack event handlers with dynamic configuration updates
pub struct DynamicSlackHandlerManager {
    // LocalConfigStore reference (for core configuration data)
    local_config_store: SharedLocalConfigStore,
    execution_ref_recorder: shared::SharedExecutionRefRecorder,

    // Heavyweight cache layer (regex, parsed data)
    slack_handler_cache: Arc<RwLock<SlackHandlerCache>>,

    // Socket Mode listener (will be implemented in Phase 4.2)
    socket_mode_listener: Option<JoinHandle<Result<()>>>,

    // Shutdown signal
    shutdown_sender: Option<oneshot::Sender<()>>,

    // Running state
    is_running: Arc<AtomicBool>,
}

impl DynamicSlackHandlerManager {
    /// Create new DynamicSlackHandlerManager
    pub fn new(local_config_store: SharedLocalConfigStore) -> Self {
        Self::new_with_recorder(local_config_store, shared::noop_execution_ref_recorder())
    }

    pub fn new_with_recorder(
        local_config_store: SharedLocalConfigStore,
        execution_ref_recorder: shared::SharedExecutionRefRecorder,
    ) -> Self {
        let slack_handler_cache = Arc::new(RwLock::new(SlackHandlerCache::new()));

        Self {
            local_config_store,
            execution_ref_recorder,
            slack_handler_cache,
            socket_mode_listener: None,
            shutdown_sender: None,
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create with initial configuration
    /// Loads initial handlers and compiles their patterns
    pub fn new_with_initial_config(
        initial_handlers: Vec<SlackEventHandler>,
        local_config_store: SharedLocalConfigStore,
    ) -> Result<Self> {
        let manager = Self::new(local_config_store);

        // Initialize handler cache
        manager.initialize_handler_cache(&initial_handlers)?;

        tracing::info!(
            "DynamicSlackHandlerManager initialized with {} handlers",
            initial_handlers.len()
        );

        Ok(manager)
    }

    /// Initialize handler cache with handlers
    fn initialize_handler_cache(&self, handlers: &[SlackEventHandler]) -> Result<()> {
        let mut cache = self
            .slack_handler_cache
            .write()
            .map_err(|e| anyhow::anyhow!("Failed to write lock cache: {}", e))?;

        for handler in handlers {
            if let Err(e) = cache.compile_and_cache_patterns(handler) {
                tracing::warn!(
                    "Failed to compile patterns for handler '{}' (id={:?}): {}. Skipping this handler.",
                    handler.data.as_ref().map(|d| d.name.as_str()).unwrap_or("unknown"),
                    handler.id.as_ref().map(|id| id.value),
                    e
                );
            }
        }

        Ok(())
    }

    /// Start the manager
    /// Loads initial configuration from LocalConfigStore and starts Socket Mode listener
    pub async fn start(&mut self) -> Result<()> {
        if self.is_running.load(Ordering::SeqCst) {
            tracing::warn!("DynamicSlackHandlerManager is already running");
            return Ok(());
        }

        // Load initial configuration from LocalConfigStore
        let handlers: Vec<SlackEventHandler> = {
            let store = self
                .local_config_store
                .read()
                .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;
            store
                .get_all_slack_event_handlers()
                .into_iter()
                .cloned()
                .collect()
        };

        // Initialize cache with loaded handlers
        self.initialize_handler_cache(&handlers)?;

        self.is_running.store(true, Ordering::SeqCst);

        // Get enabled handlers count
        let enabled_count = {
            let store = self
                .local_config_store
                .read()
                .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;
            store.get_enabled_slack_event_handlers().len()
        };

        tracing::info!(
            "DynamicSlackHandlerManager started with {} enabled handlers (total: {})",
            enabled_count,
            handlers.len()
        );

        // Start Socket Mode listener if tokens are available
        self.start_socket_mode_listener().await?;

        Ok(())
    }

    /// Start Socket Mode listener
    /// Reads tokens from environment variables and starts the listener
    async fn start_socket_mode_listener(&mut self) -> Result<()> {
        // Get tokens from environment variables
        let app_token = match std::env::var("SLACK_APP_TOKEN") {
            Ok(token) => token,
            Err(_) => {
                tracing::warn!(
                    "SLACK_APP_TOKEN not set, Socket Mode listener will not start. \
                    Set SLACK_APP_TOKEN environment variable to enable Slack event handling."
                );
                return Ok(());
            }
        };

        if app_token.is_empty() {
            tracing::warn!(
                "SLACK_APP_TOKEN is empty, Socket Mode listener will not start. \
                Set SLACK_APP_TOKEN environment variable to enable Slack event handling."
            );
            return Ok(());
        }

        tracing::info!("Starting Socket Mode listener with SLACK_APP_TOKEN");

        // Create event matcher and handler executor
        let event_matcher = Arc::new(crate::event_matcher::EventMatcher::new(
            self.local_config_store.clone(),
            self.slack_handler_cache.clone(),
        ));

        let handler_executor = Arc::new(crate::handler_executor::HandlerExecutor::new(
            self.local_config_store.clone(),
            self.execution_ref_recorder.clone(),
        ));

        // Create Socket Mode listener
        let listener = Arc::new(
            crate::socket_mode::SocketModeListener::new(app_token, event_matcher, handler_executor)
                .context("Failed to create Socket Mode listener")?,
        );

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        self.shutdown_sender = Some(shutdown_tx);

        // Start listener in background task
        let listener_clone = listener.clone();
        let handle = tokio::spawn(async move {
            listener_clone
                .start(shutdown_rx)
                .await
                .context("Socket Mode listener failed")
        });

        self.socket_mode_listener = Some(handle);

        tracing::info!("Socket Mode listener started successfully");

        Ok(())
    }

    /// Stop the manager
    pub async fn stop(&mut self) -> Result<()> {
        if !self.is_running.load(Ordering::SeqCst) {
            tracing::warn!("DynamicSlackHandlerManager is not running");
            return Ok(());
        }

        self.is_running.store(false, Ordering::SeqCst);

        // Send shutdown signal if Socket Mode listener is running
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }

        // Wait for listener to stop
        if let Some(listener) = self.socket_mode_listener.take() {
            match listener.await {
                Ok(Ok(())) => {
                    tracing::info!("Socket Mode listener stopped successfully");
                }
                Ok(Err(e)) => {
                    tracing::error!("Socket Mode listener stopped with error: {}", e);
                }
                Err(e) => {
                    tracing::error!("Failed to join Socket Mode listener: {}", e);
                }
            }
        }

        tracing::info!("DynamicSlackHandlerManager stopped");

        Ok(())
    }

    /// Update handler cache from configuration change event
    /// Called when SlackEventHandler is created/updated/deleted
    pub async fn update_handler_cache_from_event(
        &self,
        event: &ConfigChangeEventWrapper,
    ) -> Result<()> {
        use proto::jobworkerp_conductor::data::ChangeAction;

        let action = event.action();
        let handler_event = event
            .as_slack_event_handler()
            .context("Not a SlackEventHandler event")?;

        match action {
            ChangeAction::Created | ChangeAction::Updated => {
                if let (Some(id), Some(data)) = (&handler_event.id, &handler_event.data) {
                    let handler = SlackEventHandler {
                        id: Some(*id),
                        data: Some(data.clone()),
                    };

                    let mut cache = self
                        .slack_handler_cache
                        .write()
                        .map_err(|e| anyhow::anyhow!("Failed to write lock cache: {}", e))?;

                    // Invalidate old cache first
                    cache.invalidate_handler(id);

                    // Compile and cache new patterns
                    if let Err(e) = cache.compile_and_cache_patterns(&handler) {
                        tracing::error!(
                            "Failed to compile patterns for handler '{}' (id={}): {}",
                            data.name,
                            id.value,
                            e
                        );
                        return Err(e);
                    }

                    tracing::info!(
                        "Updated cache for SlackEventHandler: '{}' (id={}, action={:?})",
                        data.name,
                        id.value,
                        action
                    );
                }
            }
            ChangeAction::Deleted => {
                if let Some(id) = &handler_event.id {
                    let mut cache = self
                        .slack_handler_cache
                        .write()
                        .map_err(|e| anyhow::anyhow!("Failed to write lock cache: {}", e))?;

                    cache.invalidate_handler(id);

                    tracing::info!(
                        "Removed cache for SlackEventHandler: '{}' (id={})",
                        handler_event.name,
                        id.value
                    );
                }
            }
            _ => {
                tracing::debug!("Ignoring action: {:?}", action);
            }
        }

        Ok(())
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> Result<crate::config_cache::CacheStats> {
        let cache = self
            .slack_handler_cache
            .read()
            .map_err(|e| anyhow::anyhow!("Failed to read lock cache: {}", e))?;

        Ok(cache.stats())
    }

    /// Get running state
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Get enabled handlers count
    pub fn get_enabled_handlers_count(&self) -> Result<usize> {
        let store = self
            .local_config_store
            .read()
            .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;

        Ok(store.get_enabled_slack_event_handlers().len())
    }

    /// Get total handlers count
    pub fn get_total_handlers_count(&self) -> Result<usize> {
        let store = self
            .local_config_store
            .read()
            .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;

        Ok(store.get_all_slack_event_handlers().len())
    }

    /// Get event matcher (for testing)
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn get_event_matcher(&self) -> crate::event_matcher::EventMatcher {
        crate::event_matcher::EventMatcher::new(
            self.local_config_store.clone(),
            self.slack_handler_cache.clone(),
        )
    }

    /// Get handler executor (for testing)
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn get_handler_executor(&self) -> crate::handler_executor::HandlerExecutor {
        crate::handler_executor::HandlerExecutor::new(
            self.local_config_store.clone(),
            self.execution_ref_recorder.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::jobworkerp_conductor::data::{
        JobworkerpServerId, SlackEventHandlerData, SlackEventHandlerId,
    };
    use shared::LocalConfigStore;

    fn create_test_handler(id: i64, name: &str, enabled: bool) -> SlackEventHandler {
        SlackEventHandler {
            id: Some(SlackEventHandlerId { value: id }),
            data: Some(SlackEventHandlerData {
                name: name.to_string(),
                enabled,
                message_pattern: Some("test.*".to_string()),
                jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
                workflow_url: "http://example.com/workflow.yaml".to_string(),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_manager_creation() {
        let store = LocalConfigStore::default();
        let local_config_store = Arc::new(RwLock::new(store));

        let manager = DynamicSlackHandlerManager::new(local_config_store);

        assert!(!manager.is_running());
        assert_eq!(manager.get_enabled_handlers_count().unwrap(), 0);
    }

    #[test]
    fn test_manager_with_initial_config() {
        let mut store = LocalConfigStore::default();
        let handler1 = create_test_handler(1, "handler1", true);
        let handler2 = create_test_handler(2, "handler2", true);

        store.upsert_slack_event_handler(handler1.clone()).unwrap();
        store.upsert_slack_event_handler(handler2.clone()).unwrap();

        let local_config_store = Arc::new(RwLock::new(store));

        let manager = DynamicSlackHandlerManager::new_with_initial_config(
            vec![handler1, handler2],
            local_config_store,
        )
        .unwrap();

        let stats = manager.get_cache_stats().unwrap();
        assert_eq!(stats.message_pattern_count, 2);
    }

    #[tokio::test]
    async fn test_manager_start_stop() {
        let mut store = LocalConfigStore::default();
        let handler = create_test_handler(1, "test_handler", true);
        store.upsert_slack_event_handler(handler).unwrap();

        let local_config_store = Arc::new(RwLock::new(store));

        let mut manager = DynamicSlackHandlerManager::new(local_config_store);

        assert!(!manager.is_running());

        manager.start().await.unwrap();
        assert!(manager.is_running());
        assert_eq!(manager.get_enabled_handlers_count().unwrap(), 1);

        // Note: start() and stop() require &mut self for socket_mode_listener management
        manager.stop().await.unwrap();
        assert!(!manager.is_running());
    }

    #[tokio::test]
    async fn test_update_handler_cache_from_event() {
        let store = LocalConfigStore::default();
        let local_config_store = Arc::new(RwLock::new(store));

        let manager = DynamicSlackHandlerManager::new(local_config_store);

        let handler = create_test_handler(1, "test_handler", true);

        // Create event
        let event = shared::config_events_proto::ConfigChangeEventWrapper::create_slack_event_handler_created(
            "test_handler".to_string(),
            handler.id,
            handler.data.clone(),
            None,
        );

        manager
            .update_handler_cache_from_event(&event)
            .await
            .unwrap();

        let stats = manager.get_cache_stats().unwrap();
        assert_eq!(stats.message_pattern_count, 1);
    }

    #[tokio::test]
    async fn test_delete_handler_invalidates_cache() {
        let store = LocalConfigStore::default();
        let local_config_store = Arc::new(RwLock::new(store));

        let manager = DynamicSlackHandlerManager::new(local_config_store);

        let handler = create_test_handler(1, "test_handler", true);

        // Create event
        let create_event = shared::config_events_proto::ConfigChangeEventWrapper::create_slack_event_handler_created(
            "test_handler".to_string(),
            handler.id,
            handler.data.clone(),
            None,
        );

        manager
            .update_handler_cache_from_event(&create_event)
            .await
            .unwrap();

        let stats = manager.get_cache_stats().unwrap();
        assert_eq!(stats.message_pattern_count, 1);

        // Delete event
        let delete_event =
            shared::config_events_proto::ConfigChangeEventWrapper::create_slack_event_handler_deleted(
                "test_handler".to_string(),
                handler.id,
            );

        manager
            .update_handler_cache_from_event(&delete_event)
            .await
            .unwrap();

        let stats = manager.get_cache_stats().unwrap();
        assert_eq!(stats.message_pattern_count, 0);
    }

    #[tokio::test]
    async fn test_start_loads_handlers_from_local_config_store() {
        let mut store = LocalConfigStore::default();
        let handler1 = create_test_handler(1, "handler1", true);
        let handler2 = create_test_handler(2, "handler2", false);
        let handler3 = create_test_handler(3, "handler3", true);

        store.upsert_slack_event_handler(handler1).unwrap();
        store.upsert_slack_event_handler(handler2).unwrap();
        store.upsert_slack_event_handler(handler3).unwrap();

        let local_config_store = Arc::new(RwLock::new(store));

        let mut manager = DynamicSlackHandlerManager::new(local_config_store);

        manager.start().await.unwrap();

        assert_eq!(manager.get_total_handlers_count().unwrap(), 3);
        assert_eq!(manager.get_enabled_handlers_count().unwrap(), 2);

        let stats = manager.get_cache_stats().unwrap();
        assert_eq!(stats.message_pattern_count, 3); // All handlers cached (enabled + disabled)
    }
}
