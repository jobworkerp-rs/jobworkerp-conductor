// Integration tests for SlackEventHandler
// Tests Socket Mode integration, configuration change flow, and event processing

use anyhow::Result;
use proto::jobworkerp_conductor::data::{
    JobworkerpServer, JobworkerpServerData, JobworkerpServerId, SlackEventHandler,
    SlackEventHandlerData, SlackEventHandlerId,
};
use shared::config_events_proto::ConfigChangeEventWrapper;
use shared::LocalConfigStore;
use slack_event_handler::DynamicSlackHandlerManager;
use std::sync::{Arc, RwLock};

/// Helper function to create a test handler
fn create_test_handler(
    id: i64,
    name: &str,
    enabled: bool,
    pattern: Option<&str>,
) -> SlackEventHandler {
    use proto::jobworkerp_conductor::data::ReactionOperation;

    SlackEventHandler {
        id: Some(SlackEventHandlerId { value: id }),
        data: Some(SlackEventHandlerData {
            name: name.to_string(),
            description: format!("Test handler: {}", name),
            enabled,
            slack_channel_id: Some("C123456".to_string()),
            message_pattern: pattern.map(|p| p.to_string()),
            mention_required: false,
            reaction_names: None,
            reaction_operation: ReactionOperation::Unspecified as i32,
            reaction_user_filter: None,
            jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
            workflow_url: "http://localhost:8080/workflow.yaml".to_string(),
            channel: "default".to_string(),
            timeout_sec: Some(3600),
            args: None,
            execution_target: None,
            created_at: 1000,
            updated_at: 1000,
        }),
    }
}

/// Helper function to create a test JobworkerpServer
fn create_test_jobworkerp_server(id: i64, name: &str) -> JobworkerpServer {
    JobworkerpServer {
        id: Some(JobworkerpServerId { value: id }),
        data: Some(JobworkerpServerData {
            name: name.to_string(),
            host: "localhost".to_string(),
            port: "9000".to_string(),
            ssl_enabled: false,
            description: Some(format!("Test server: {}", name)),
            enabled: true,
            created_at: 1000,
            updated_at: 1000,
        }),
    }
}

/// Test: Manager creation with initial configuration
#[tokio::test]
async fn test_manager_creation_with_initial_config() -> Result<()> {
    let mut store = LocalConfigStore::default();

    // Add test handlers
    let handler1 = create_test_handler(1, "handler1", true, Some("^deploy"));
    let handler2 = create_test_handler(2, "handler2", true, Some("^test"));
    store.upsert_slack_event_handler(handler1)?;
    store.upsert_slack_event_handler(handler2)?;

    // Add JobworkerpServer
    let server = create_test_jobworkerp_server(1, "default");
    store.upsert_jobworkerp_server(server)?;

    let local_config_store = Arc::new(RwLock::new(store));

    let manager = DynamicSlackHandlerManager::new(local_config_store);

    // Verify cache stats before start
    let stats = manager.get_cache_stats()?;
    assert_eq!(stats.message_pattern_count, 0); // Cache not loaded yet

    Ok(())
}

/// Test: Manager lifecycle (start/stop)
#[tokio::test]
async fn test_manager_lifecycle() -> Result<()> {
    let mut store = LocalConfigStore::default();

    let handler = create_test_handler(1, "test_handler", true, Some("^hello"));
    store.upsert_slack_event_handler(handler)?;

    let server = create_test_jobworkerp_server(1, "default");
    store.upsert_jobworkerp_server(server)?;

    let local_config_store = Arc::new(RwLock::new(store));

    let mut manager = DynamicSlackHandlerManager::new(local_config_store);

    // Initial state
    assert!(!manager.is_running());

    // Note: start() will fail without SLACK_APP_TOKEN, but we can test the flow
    // In real integration tests, you would set the environment variable
    match manager.start().await {
        Ok(_) => {
            assert!(manager.is_running());
            assert_eq!(manager.get_enabled_handlers_count()?, 1);

            manager.stop().await?;
            assert!(!manager.is_running());
        }
        Err(e) => {
            // Expected if SLACK_APP_TOKEN is not set
            println!("Start failed (expected without SLACK_APP_TOKEN): {:?}", e);
        }
    }

    Ok(())
}

/// Test: Configuration change event processing (cache only)
/// Note: In actual system, EventHandlerServerManager updates both LocalConfigStore and cache
#[tokio::test]
async fn test_config_change_event_processing() -> Result<()> {
    let store = LocalConfigStore::default();
    let local_config_store = Arc::new(RwLock::new(store));

    let manager = DynamicSlackHandlerManager::new(local_config_store.clone());

    let handler = create_test_handler(1, "new_handler", true, Some("^urgent"));

    // Create event
    let event = ConfigChangeEventWrapper::create_slack_event_handler_created(
        "new_handler".to_string(),
        handler.id,
        handler.data.clone(),
        None,
    );

    // Update LocalConfigStore (simulating EventHandlerServerManager behavior)
    {
        let mut store = local_config_store.write().unwrap();
        store.upsert_slack_event_handler(handler.clone())?;
    }

    // Process event (updates cache only)
    manager.update_handler_cache_from_event(&event).await?;

    // Verify cache updated
    let stats = manager.get_cache_stats()?;
    assert_eq!(stats.message_pattern_count, 1);

    // Verify LocalConfigStore updated
    {
        let store = local_config_store.read().unwrap();
        let stored_handler = store.find_slack_event_handler_by_name("new_handler");
        assert!(stored_handler.is_some());
    }

    Ok(())
}

/// Test: Handler update flow
#[tokio::test]
async fn test_handler_update_flow() -> Result<()> {
    let mut store = LocalConfigStore::default();
    let handler = create_test_handler(1, "updatable_handler", true, Some("^old_pattern"));
    store.upsert_slack_event_handler(handler.clone())?;

    let local_config_store = Arc::new(RwLock::new(store));
    let manager = DynamicSlackHandlerManager::new(local_config_store.clone());

    // Initial cache load
    let create_event = ConfigChangeEventWrapper::create_slack_event_handler_created(
        "updatable_handler".to_string(),
        handler.id,
        handler.data.clone(),
        None,
    );
    manager
        .update_handler_cache_from_event(&create_event)
        .await?;

    let initial_stats = manager.get_cache_stats()?;
    assert_eq!(initial_stats.message_pattern_count, 1);

    // Update handler with new pattern
    let mut updated_handler = handler;
    updated_handler.data.as_mut().unwrap().message_pattern = Some("^new_pattern".to_string());

    let update_event = ConfigChangeEventWrapper::create_slack_event_handler_updated(
        "updatable_handler".to_string(),
        updated_handler.id,
        updated_handler.data.clone(),
        None,
    );
    manager
        .update_handler_cache_from_event(&update_event)
        .await?;

    // Verify cache re-compiled with new pattern
    let updated_stats = manager.get_cache_stats()?;
    assert_eq!(updated_stats.message_pattern_count, 1);
    assert!(updated_stats.version > initial_stats.version);

    Ok(())
}

/// Test: Handler deletion flow
#[tokio::test]
async fn test_handler_deletion_flow() -> Result<()> {
    let mut store = LocalConfigStore::default();
    let handler = create_test_handler(1, "deletable_handler", true, Some("^delete_me"));
    store.upsert_slack_event_handler(handler.clone())?;

    let local_config_store = Arc::new(RwLock::new(store));
    let manager = DynamicSlackHandlerManager::new(local_config_store.clone());

    // Create handler (cache only, LocalConfigStore already has it)
    let create_event = ConfigChangeEventWrapper::create_slack_event_handler_created(
        "deletable_handler".to_string(),
        handler.id,
        handler.data.clone(),
        None,
    );
    manager
        .update_handler_cache_from_event(&create_event)
        .await?;

    let initial_stats = manager.get_cache_stats()?;
    assert_eq!(initial_stats.message_pattern_count, 1);

    // Delete handler from LocalConfigStore (simulating EventHandlerServerManager)
    {
        let mut store = local_config_store.write().unwrap();
        store.remove_slack_event_handler(&handler.id.unwrap());
    }

    // Delete handler cache
    let delete_event = ConfigChangeEventWrapper::create_slack_event_handler_deleted(
        "deletable_handler".to_string(),
        handler.id,
    );
    manager
        .update_handler_cache_from_event(&delete_event)
        .await?;

    // Verify cache cleared
    let after_delete_stats = manager.get_cache_stats()?;
    assert_eq!(after_delete_stats.message_pattern_count, 0);

    // Verify LocalConfigStore updated
    {
        let store = local_config_store.read().unwrap();
        let deleted_handler = store.find_slack_event_handler_by_name("deletable_handler");
        assert!(deleted_handler.is_none());
    }

    Ok(())
}

/// Test: Multiple handlers configuration
#[tokio::test]
async fn test_multiple_handlers_management() -> Result<()> {
    let mut store = LocalConfigStore::default();

    // Add multiple handlers
    let handler1 = create_test_handler(1, "handler1", true, Some("^deploy"));
    let handler2 = create_test_handler(2, "handler2", true, Some("^test"));
    let handler3 = create_test_handler(3, "handler3", false, Some("^disabled")); // disabled

    store.upsert_slack_event_handler(handler1.clone())?;
    store.upsert_slack_event_handler(handler2.clone())?;
    store.upsert_slack_event_handler(handler3.clone())?;

    let local_config_store = Arc::new(RwLock::new(store));
    let manager = DynamicSlackHandlerManager::new(local_config_store);

    // Load all handlers (cache only - manager compiles patterns for all handlers including disabled)
    let create_event1 = ConfigChangeEventWrapper::create_slack_event_handler_created(
        "handler1".to_string(),
        handler1.id,
        handler1.data.clone(),
        None,
    );
    let create_event2 = ConfigChangeEventWrapper::create_slack_event_handler_created(
        "handler2".to_string(),
        handler2.id,
        handler2.data.clone(),
        None,
    );
    let create_event3 = ConfigChangeEventWrapper::create_slack_event_handler_created(
        "handler3".to_string(),
        handler3.id,
        handler3.data.clone(),
        None,
    );

    manager
        .update_handler_cache_from_event(&create_event1)
        .await?;
    manager
        .update_handler_cache_from_event(&create_event2)
        .await?;
    manager
        .update_handler_cache_from_event(&create_event3)
        .await?;

    // Verify cache (all handlers should have compiled patterns, even if disabled)
    let stats = manager.get_cache_stats()?;
    assert_eq!(stats.message_pattern_count, 3); // All handlers cached

    // Verify enabled handler count (only enabled handlers)
    assert_eq!(manager.get_enabled_handlers_count()?, 2);

    Ok(())
}

/// Test: Cache invalidation on pattern update
#[tokio::test]
async fn test_cache_invalidation_on_pattern_update() -> Result<()> {
    let mut store = LocalConfigStore::default();
    let handler = create_test_handler(1, "pattern_handler", true, Some("^pattern1"));
    store.upsert_slack_event_handler(handler.clone())?;

    let local_config_store = Arc::new(RwLock::new(store));
    let manager = DynamicSlackHandlerManager::new(local_config_store);

    // Initial pattern
    let create_event = ConfigChangeEventWrapper::create_slack_event_handler_created(
        "pattern_handler".to_string(),
        handler.id,
        handler.data.clone(),
        None,
    );
    manager
        .update_handler_cache_from_event(&create_event)
        .await?;

    let initial_version = manager.get_cache_stats()?.version;

    // Update pattern
    let mut updated_handler = handler;
    updated_handler.data.as_mut().unwrap().message_pattern = Some("^pattern2".to_string());

    let update_event = ConfigChangeEventWrapper::create_slack_event_handler_updated(
        "pattern_handler".to_string(),
        updated_handler.id,
        updated_handler.data.clone(),
        None,
    );
    manager
        .update_handler_cache_from_event(&update_event)
        .await?;

    // Verify version incremented
    let updated_version = manager.get_cache_stats()?.version;
    assert!(updated_version > initial_version);

    Ok(())
}

/// Test: Concurrent configuration changes
#[tokio::test]
async fn test_concurrent_config_changes() -> Result<()> {
    let store = LocalConfigStore::default();
    let local_config_store = Arc::new(RwLock::new(store));
    let manager = Arc::new(DynamicSlackHandlerManager::new(local_config_store.clone()));

    // Spawn multiple concurrent updates
    let mut handles = vec![];

    for i in 1..=10 {
        let manager_clone = Arc::clone(&manager);
        let handle = tokio::spawn(async move {
            let handler = create_test_handler(i, &format!("handler{}", i), true, Some("^test"));
            let event = ConfigChangeEventWrapper::create_slack_event_handler_created(
                format!("handler{}", i),
                handler.id,
                handler.data.clone(),
                None,
            );
            manager_clone.update_handler_cache_from_event(&event).await
        });
        handles.push(handle);
    }

    // Wait for all updates
    for handle in handles {
        handle.await??;
    }

    // Verify all handlers cached
    let stats = manager.get_cache_stats()?;
    assert_eq!(stats.message_pattern_count, 10);

    Ok(())
}

/// Test: Invalid regex pattern handling
#[tokio::test]
async fn test_invalid_regex_pattern_handling() -> Result<()> {
    let store = LocalConfigStore::default();
    let local_config_store = Arc::new(RwLock::new(store));
    let manager = DynamicSlackHandlerManager::new(local_config_store);

    // Create handler with invalid regex
    let mut handler = create_test_handler(1, "invalid_pattern_handler", true, None);
    handler.data.as_mut().unwrap().message_pattern = Some("[invalid(regex".to_string()); // Invalid regex

    let event = ConfigChangeEventWrapper::create_slack_event_handler_created(
        "invalid_pattern_handler".to_string(),
        handler.id,
        handler.data.clone(),
        None,
    );

    // Should handle gracefully (log warning, skip caching)
    let result = manager.update_handler_cache_from_event(&event).await;

    // Should not panic, but may return error or skip caching
    if let Err(e) = result {
        println!("Expected error for invalid regex: {:?}", e);
    }

    // Verify no cache entry for invalid pattern
    let stats = manager.get_cache_stats()?;
    assert_eq!(stats.message_pattern_count, 0);

    Ok(())
}
